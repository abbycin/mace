use core::str;

use logger::Logger;
use mace::{Mace, OpCode, Options, RandomPath};

fn to_str(x: &[u8]) -> &str {
    str::from_utf8(x).unwrap()
}

fn main() -> Result<(), OpCode> {
    Logger::init().add_console();
    log::set_max_level(log::LevelFilter::Info);
    let path = RandomPath::tmp();
    let opt = Options::new(&*path);
    let m = Mace::new(opt)?;

    m.put("foo", "baz")?;
    m.put("114", "514")?;

    log::info!("===>> {}", to_str(m.get("114")?.put()));
    log::info!("===>> {}", to_str(m.get("foo")?.put()));

    m.del("foo")?;

    let x = m.get("foo")?;
    assert!(x.is_del());

    Ok(())
}
