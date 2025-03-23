use mace::{IsolationLevel, Mace, OpCode, Options};

fn main() -> Result<(), OpCode> {
    let path = std::env::temp_dir().join("mace");
    let _ = std::fs::remove_dir_all(&path);
    let opt = Options::new(path);
    let db = Mace::new(opt)?;
    let tx = db.default();

    // start a read-write Txn
    tx.begin(IsolationLevel::SI, |kv| {
        kv.put("foo", "bar")?;
        kv.put("fool", "+1s")?;
        kv.put("foolish", "elder")?;

        let r = kv.get("foo")?;
        assert_eq!(r.data(), "bar".as_bytes());
        kv.del("foolish")?;
        kv.commit()
    })?;

    // rollback
    tx.begin(IsolationLevel::SI, |kv| {
        kv.put("mo", "ha")?;
        kv.rollback()
    })?;

    // start a read-only Txn
    tx.view(IsolationLevel::SI, |view| {
        let r = view.get("foo")?;
        assert_eq!(r.data(), "bar".as_bytes());
        let r = view.get("mo");
        assert_eq!(r.err().unwrap(), OpCode::NotFound);
        Ok(())
    })?;

    // prefix scan
    tx.view(IsolationLevel::SI, |view| {
        let r = view.get("foolish");
        assert!(r.is_err() && r.err().unwrap() == OpCode::NotFound);
        let iter = view.seek("foo");
        assert_eq!(iter.count(), 2);
        Ok(())
    })
}
