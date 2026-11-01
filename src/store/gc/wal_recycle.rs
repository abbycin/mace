use super::GarbageCollector;
#[cfg(feature = "metrics")]
use crate::utils::observe::CounterMetric;
use crate::{
    cc::{context::Context, log::Logging},
    meta::{Manifest, WalRecycleIntent},
    must_ok,
    utils::{Handle, options::Options},
};

impl GarbageCollector {
    pub(super) fn process_wal_clean(&mut self) {
        self.process_wal_clean_with_manifest(self.store.manifest);
    }

    pub(super) fn process_wal_clean_with_manifest(&mut self, manifest: Handle<Manifest>) {
        // suspended deletions skip the whole recycle phase, including the durable
        // intent commit: a committed intent whose files were not deleted can only be
        // completed at the next startup, so the phase must not start at all.
        let Some(_work) = manifest.deletions.begin() else {
            return;
        };
        let ctx = self.ctx;
        if ctx.opt.sync_on_write {
            self.process_shared_wal_clean(ctx, manifest);
            return;
        }
        for g in ctx.groups().iter() {
            let (oldest_id, last_ckpt_file, mut checkpoint_id) = {
                let mut logging = g.logging.lock();
                (
                    logging.oldest_wal_id(),
                    logging.checkpoint_floor().file_id,
                    g.min_active_wal_file_id(&mut logging),
                )
            };
            if let Some(min_pending_file) = ctx.min_abort_clean_file_id(g.id as u8) {
                checkpoint_id = checkpoint_id.min(min_pending_file);
            }
            checkpoint_id = checkpoint_id.min(last_ckpt_file);
            if oldest_id >= checkpoint_id {
                continue;
            }

            let intent = WalRecycleIntent {
                group_id: g.id as u8,
                from_file_id: oldest_id,
                to_file_id: checkpoint_id,
            };
            let mut logging = g.logging.lock();
            self.process_wal_recycle_intent(ctx, manifest, intent, Some(&mut logging));
        }
    }

    fn process_shared_wal_clean(&mut self, ctx: Handle<Context>, manifest: Handle<Manifest>) {
        let Some(_work) = manifest.deletions.begin() else {
            return;
        };
        let shared = ctx
            .shared_logging()
            .expect("durable mode has a shared logging");
        let mut logging = shared.lock();
        let oldest_id = logging.oldest_wal_id();
        let mut checkpoint_id = logging.checkpoint_floor().file_id;
        // retain every active, abort-clean, and checkpoint pin
        for g in ctx.groups().iter() {
            checkpoint_id = checkpoint_id.min(g.min_active_wal_file_id(&mut logging));
        }
        if let Some(min_pending_file) = ctx.min_abort_clean_file_id(Options::SHARED_ID) {
            checkpoint_id = checkpoint_id.min(min_pending_file);
        }
        if checkpoint_id == u64::MAX {
            // retain the current file as the recovery anchor
            checkpoint_id = logging.current_pos().file_id;
        }
        if oldest_id >= checkpoint_id {
            return;
        }
        let intent = WalRecycleIntent {
            group_id: Options::SHARED_ID,
            from_file_id: oldest_id,
            to_file_id: checkpoint_id,
        };
        self.process_wal_recycle_intent(ctx, manifest, intent, Some(&mut logging));
    }

    fn process_wal_recycle_intent(
        &mut self,
        ctx: Handle<Context>,
        manifest: Handle<Manifest>,
        intent: WalRecycleIntent,
        logging: Option<&mut Logging>,
    ) {
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_wal_recycle_before_intent_commit");
        let Some(intent) = manifest.commit_wal_recycle_intent(intent) else {
            return;
        };

        // [oldest_id, checkpoint_id)
        let recycled = Self::process_one_wal(ctx, intent);
        if recycled == 0 {
            assert!(
                manifest.commit_wal_recycle_done(intent),
                "wal recycle owner must complete its matching intent"
            );
            if let Some(logging) = logging {
                logging.advance_oldest_wal_id(intent.to_file_id);
            }
            return;
        }
        ctx.opt.sync_log_dir();
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_wal_recycle_after_dir_sync_before_done_commit");
        assert!(
            manifest.commit_wal_recycle_done(intent),
            "wal recycle owner must complete its matching intent"
        );
        #[cfg(feature = "failpoints")]
        crate::utils::failpoint::crash("mace_wal_recycle_after_done_commit_before_publish");
        if let Some(logging) = logging {
            logging.advance_oldest_wal_id(intent.to_file_id);
        }
        #[cfg(feature = "metrics")]
        ctx.opt
            .observer
            .counter(CounterMetric::GcWalRecycleFile, recycled);
    }

    fn process_one_wal(ctx: Handle<Context>, intent: WalRecycleIntent) -> u64 {
        let mut recycled = 0;
        // NOTE: not including `end`
        for seq in intent.from_file_id..intent.to_file_id {
            let from = ctx.opt.physical_wal_path(intent.group_id, seq);
            if !must_ok!(ctx.opt.fs.try_exists(&from), "can't stat {:?}", from) {
                continue;
            }
            log::info!("unlink {from:?}");
            must_ok!(
                ctx.opt.fs.remove_file_if_exists(&from),
                "can't remove {from:?}"
            );
            recycled += 1;
            #[cfg(feature = "failpoints")]
            crate::utils::failpoint::crash("mace_wal_recycle_after_remove_before_dir_sync");
        }
        recycled
    }
}
