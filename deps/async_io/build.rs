use std::path::PathBuf;

fn main() {
    println!("cargo:rustc-rerun-if-changed=.");
    println!("cargo:rustc-link-search=.");
    println!("cargo:rustc-link-lib=dylib=asyncio"); // my wrapper
    println!("cargo:rustc-link-lib=dylib=aio"); // libaio-dev
    cc::Build::new().file("lib/asyncio.c").compile("asyncio");
    let binding = bindgen::Builder::default()
        .header("lib/asyncio.h")
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .generate()
        .expect("can't generate bindings");
    let out = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    binding
        .write_to_file(out.join("bindings.rs"))
        .expect("can't write bindings");
}
