extern crate cbindgen;

use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    let changed = cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_pragma_once(true)
        .with_language(cbindgen::Language::C)
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file("include/sdk-core-bridge.h");

    // If this changed and an env var disallows change, error
    if let Ok(env_val) = env::var("TEMPORAL_SDK_CORE_BRIDGE_FFI_DISABLE_HEADER_CHANGE") {
        if changed && env_val == "true" {
            println!(
                "cargo:warning=bridge-ffi's header file changed unexpectedly from what's on disk"
            );
            std::process::exit(1);
        }
    }
}
