use mace::{Mace, OpCode, Options, RandomPath};
use rand::{self, seq::SliceRandom};

#[test]
fn test_tree() -> Result<(), OpCode> {
    let path = RandomPath::tmp();
    let mut opt = Options::new(&*path);
    opt.page_size_threshold = 1024;
    let _ = std::fs::remove_dir_all(&opt.db_root);

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
        assert_eq!(v.put(), &x.as_bytes());
    }

    Ok(())
}
