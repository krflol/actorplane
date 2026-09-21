use std::env;
use std::fs;

// libfuzzer-sys references this symbol even though the probe drives the hook
// directly instead of entering the fuzzer loop.
#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
extern "C" fn rust_fuzzer_test_input(_input: &[u8]) -> i32 {
    0
}

fn main() {
    // libfuzzer-sys installs its aborting hook here; our helper chains to it
    // after saving the input, matching the real fuzz-target startup order.
    let _ = libfuzzer_sys::initialize(std::ptr::null(), std::ptr::null());
    actorplane_fuzz::install_panic_capture("panic_capture_probe");

    let path = env::args().nth(1).expect("input file argument");
    let input = fs::read(path).expect("read input file");
    actorplane_fuzz::record_input(&input);
    panic!("intentional fuzz panic probe");
}
