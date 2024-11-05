use mace::{Mace, OpCode, Options};
use rand::{self, seq::SliceRandom};

#[test]
fn test_tree() -> Result<(), OpCode> {
    let mut opt = Options::default();
    opt.page_size_threshold = 1024;
    opt.db_path = "/tmp/tree".into();
    let _ = std::fs::remove_dir_all(&opt.db_path);

    let m = Mace::new(opt)?;
    let mut pairs = Vec::new();
    let cnt = 1000;
    let mut rng = rand::thread_rng();

    for i in 0..cnt {
        pairs.push((format!("key{i}"), format!("val{i}")));
    }

    pairs.shuffle(&mut rng);

    for (k, v) in &pairs {
        m.put(k, v)?;
    }

    for (k, x) in &pairs {
        let v = m.get(k).expect("failed to get value");
        assert_eq!(v.to_vec().as_slice(), x.as_bytes());
    }

    Ok(())
}
