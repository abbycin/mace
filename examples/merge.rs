use mace::{BucketOptions, Mace, OpCode, Options, u64_add_operator};

fn main() -> Result<(), OpCode> {
    let path = std::env::temp_dir().join(format!("mace_merge_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);

    let db = Mace::new(Options::new(&path).validate()?)?;
    let bucket = db.new_bucket(
        "counters",
        BucketOptions {
            merge_operator: u64_add_operator(),
            ..BucketOptions::default()
        },
    )?;

    let tx = bucket.begin()?;
    tx.put("requests", 10u64.to_le_bytes())?;
    tx.commit()?;

    let tx = bucket.begin()?;
    tx.merge("requests", 5u64.to_le_bytes())?;
    tx.merge("requests", 7u64.to_le_bytes())?;
    tx.commit()?;

    let view = bucket.view()?;
    let value = view.get("requests")?;
    let requests = u64::from_le_bytes(value.slice().try_into().expect("u64 value"));
    assert_eq!(requests, 22);
    println!("requests: {requests}");

    Ok(())
}
