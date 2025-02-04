# mace
An embedded key-value storage engine with ACID support. Although it is still under development, you can try it out

```rust
use mace::{IsolationLevel, Mace, OpCode, Options};

fn main() -> Result<(), OpCode> {
    let opt = Options::new(std::env::temp_dir().join("mace"));
    let db = Mace::new(opt)?;
    let tx = db.default();

    // start a read-write Txn
    tx.begin(IsolationLevel::SI, |kv| {
        kv.put("foo", "bar")?;
        kv.commit()?;

        let r = kv.get("foo")?;
        assert_eq!(r.data(), "bar".as_bytes());
        Ok(())
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
    })
}
```
