use mace::{Mace, OpCode, Options};

fn main() -> Result<(), OpCode> {
    let path = std::env::temp_dir().join("mace");
    let _ = std::fs::remove_dir_all(&path);
    let opt = Options::new(path);
    let db = Mace::new(opt)?;

    // start a read-write Txn
    let kv = db.begin()?;
    kv.put("foo", "bar")?;
    kv.put("fool", "+1s")?;
    kv.put("foolish", "elder")?;

    // can't create two identical keys
    let r = kv.put("foolish", "114514").err();
    assert_eq!(r.unwrap(), OpCode::AbortTx);

    // use `update` for exist key or use `upsert` when unsure
    let r = kv.update("foolish", "114514").unwrap();
    assert_eq!(r.data(), "elder".as_bytes());

    let r = kv.get("foo")?;
    assert_eq!(r.data(), "bar".as_bytes());
    kv.del("foolish")?;
    kv.commit()?;

    // rollback
    let kv = db.begin()?;
    kv.put("mo", "ha")?;
    kv.rollback()?;

    // start a read-only Txn
    let view = db.view()?;
    let r = view.get("foo")?;
    assert_eq!(r.data(), "bar".as_bytes());
    let r = view.get("mo");
    assert_eq!(r.err().unwrap(), OpCode::NotFound);

    // prefix scan
    let r = view.get("foolish");
    assert!(r.is_err() && r.err().unwrap() == OpCode::NotFound);
    let iter = view.seek("foo");
    assert_eq!(iter.count(), 2);

    Ok(())
}
