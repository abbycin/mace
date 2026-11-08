//! checkpoint task, payload file building, and payload readers

mod checkpoint_task;
mod file_builder;
mod file_reader;

#[cfg(feature = "extra_check")]
pub(crate) use checkpoint_task::first_live;
#[allow(unused_imports)]
pub(crate) use checkpoint_task::{
    AddrSet, CheckpointTask, EpochInflight, PidMap, PidSet, Snapshot, mark_root,
};
#[allow(unused_imports)]
pub(crate) use file_builder::{BuiltFile, FileBuilder, FileFooter, FileVersion, MapBuilder};
pub(crate) use file_reader::new_reader;
#[cfg(feature = "maintenance")]
pub use file_reader::try_new_reader;
pub use file_reader::{FileReader, MetaReader};

#[cfg(test)]
mod test {
    use crate::{
        Options, RandomPath,
        map::data::{FileVersion, MetaReader},
        meta::FileKind,
        types::{
            header::{NodeType, TagKind},
            refbox::BoxRef,
            traits::IHeader,
        },
        utils::{INIT_ID, compress::CompressorPool},
    };

    use super::FileBuilder;

    fn sample_pages() -> [BoxRef; 2] {
        let (pid, addr) = (114514, 1919810);
        let mut p = BoxRef::alloc(233, addr);
        p.header_mut().pid = pid;
        p.header_mut().kind = TagKind::Delta;
        p.header_mut().node_type = NodeType::Leaf;

        let (pid1, addr1) = (192, 68);
        let mut p1 = BoxRef::alloc(666, addr1);
        p1.header_mut().pid = pid1;
        p1.header_mut().kind = TagKind::Delta;
        p1.header_mut().node_type = NodeType::Leaf;
        [p, p1]
    }

    #[test]
    fn data_dump_load() {
        let path = RandomPath::tmp();
        let mut opt = Options::new(&*path);
        opt.tmp_store = true;
        let opt = opt.validate().unwrap();

        let _ = opt.create_dir();

        let [p, p1] = sample_pages();
        let addr = p.header().addr;
        let addr1 = p1.header().addr;

        let mut builder = FileBuilder::new(0, false, CompressorPool::new(), opt.fs.clone());

        builder.add(p.clone());
        builder.add(p1.clone());

        let mut file_id = INIT_ID;
        let files = builder.flush_files(
            FileKind::Data,
            opt.data_file_size,
            7,
            || {
                let id = file_id;
                file_id += 1;
                (id, opt.data_file(id))
            },
            |_| {},
            |_| {},
        );
        assert_eq!(files.len(), 1);

        let mut loader = MetaReader::new(opt.fs.as_ref(), opt.data_file(INIT_ID));
        assert_eq!(loader.version(), FileVersion::V1);

        let reloc = loader.get_reloc();
        let intervals = loader.get_interval();

        assert_eq!(reloc.len(), 2);
        assert_eq!(intervals.len(), 1);

        assert_eq!({ intervals[0].lo }, addr1);
        assert_eq!({ intervals[0].hi }, addr);

        let r = reloc
            .iter()
            .find(|x| x.key == addr)
            .expect("reloc for addr must exist");
        assert_eq!({ r.val.off }, p1.dump_len());
        assert_eq!({ r.val.raw_len }, p.dump_len() as u32);
        assert_eq!({ r.val.compressed_len }, 0);

        let r1 = reloc
            .iter()
            .find(|x| x.key == addr1)
            .expect("reloc for addr1 must exist");
        assert_eq!({ r1.val.off }, 0);
        assert_eq!({ r1.val.raw_len }, p1.dump_len() as u32);
        assert_eq!({ r1.val.compressed_len }, 0);
    }
}
