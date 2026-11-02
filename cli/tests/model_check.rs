//! randomized churn followed by the offline checker.

use mace::{BucketOptions, Mace, Options, RandomPath};
use mace_kv_cli::{check_database, leak_check_database};
use std::time::{Duration, Instant};

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

fn open(dir: &RandomPath) -> Mace {
    let mut opt = Options::new(&**dir);
    opt.sync_on_write = true;
    opt.gc_eager = true;
    opt.gc_timeout = 10;
    opt.data_garbage_ratio = 1;
    opt.blob_garbage_ratio = 1;
    Mace::new(opt.validate().expect("validate options")).expect("open mace")
}

fn gc_round(mace: &Mace) {
    let before = mace.data_gc_count() + mace.blob_gc_count();
    mace.start_gc();
    let deadline = Instant::now() + Duration::from_millis(250);
    while mace.data_gc_count() + mace.blob_gc_count() == before {
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn churn(mace: &Mace, seed: u64, rounds: usize) {
    let main = mace
        .new_bucket("churn", BucketOptions::default())
        .expect("create churn bucket");
    let mut rng = Rng::new(seed);

    for round in 0..rounds {
        let txn = main.begin().expect("begin churn txn");
        for _ in 0..4 {
            let key = format!("k{:02}", rng.below(64));
            if rng.below(4) == 0 {
                let _ = txn.del(&key);
            } else {
                let len = 8 + rng.below(2048);
                let payload = vec![(round % 251) as u8; len];
                txn.upsert(&key, &payload).expect("upsert churn key");
            }
        }
        txn.commit().expect("commit churn txn");

        if round % 8 == 0 {
            main.checkpoint_and_wait();
            gc_round(mace);
        }

        if round % 16 == 0 {
            let name = format!("tmp_{round}");
            let churn_bucket = mace
                .new_bucket(&name, BucketOptions::default())
                .expect("create temp bucket");
            let txn = churn_bucket.begin().expect("begin temp txn");
            txn.upsert("only", b"value").expect("temp upsert");
            txn.commit().expect("temp commit");
            drop(churn_bucket);
            mace.drop_bucket(&name).expect("unload temp bucket");
            mace.del_bucket(&name).expect("delete temp bucket");
        }
    }

    mace.sync().expect("sync churn");
}

#[test]
fn randomized_churn_then_offline_check_is_clean() {
    let seed: u64 = std::env::var("MACE_MODEL_SEED")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);
    let rounds: usize = std::env::var("MACE_MODEL_CHURN_ROUNDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(256);
    let dir = RandomPath::tmp();

    {
        let mace = open(&dir);
        churn(&mace, seed, rounds);
        gc_round(&mace);
        drop(mace);
    }

    {
        // one reopen pass so recovery runs before the offline check
        let mace = open(&dir);
        mace.sync().expect("sync after reopen");
        drop(mace);
    }

    let opt = Options::new(&*dir);
    let leak = leak_check_database(&opt, false);
    assert!(
        leak.report.is_ok(),
        "leak diagnostics: {:?}",
        leak.report.summary()
    );
    let report = check_database(&opt);
    assert!(report.is_ok(), "check diagnostics: {:?}", report.summary());
}
