use core::str;

use logger::Logger;
use mace::{Mace, OpCode, Options};

fn to_str(x: &Vec<u8>) -> &str {
    str::from_utf8(x.as_slice()).unwrap()
}

fn main() -> Result<(), OpCode> {
    Logger::init().add_console();
    log::set_max_level(log::LevelFilter::Info);
    let opt = Options::default();
    let _ = std::fs::remove_dir_all(&opt.db_path);
    let m = Mace::new(opt)?;

    m.put("foo", "baz")?;
    m.put("114", "514")?;

    log::info!("===>> {}", to_str(&m.get("114")?.to_vec()));
    log::info!("===>> {}", to_str(&m.get("foo")?.to_vec()));

    m.del("foo")?;

    log::info!("===>> {}", to_str(&m.get("foo")?.to_vec()));

    Ok(())
}
