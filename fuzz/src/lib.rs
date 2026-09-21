//! Small panic-artifact support shared by the coverage-guided fuzz targets.
//!
//! Inputs are copied at target entry and are written only if the process later
//! panics. The hook deliberately remains best-effort: an artifact failure must
//! never replace the original panic handling or cause recursive panics.

use std::cell::RefCell;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::panic::PanicHookInfo;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

const MAX_INPUT: usize = 65_536;

thread_local! {
    static CURRENT_INPUT: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
}

static TARGET: OnceLock<&'static str> = OnceLock::new();
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Install a panic hook that saves the most recently recorded input before
/// chaining the hook that was installed by libFuzzer.
pub fn install_panic_capture(target: &'static str) {
    if TARGET.set(target).is_err() {
        return;
    }

    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        write_artifact(info);
        previous(info);
    }));
}

/// Record the current fuzz input. Inputs larger than the bounded artifact
/// limit clear the previous value and are intentionally not saved.
pub fn record_input(data: &[u8]) {
    CURRENT_INPUT.with(|slot| {
        let Ok(mut input) = slot.try_borrow_mut() else {
            return;
        };
        *input = (data.len() <= MAX_INPUT).then(|| data.to_vec());
    });
}

fn write_artifact(_info: &PanicHookInfo<'_>) {
    let input = CURRENT_INPUT
        .try_with(|slot| slot.try_borrow().ok().and_then(|bytes| bytes.clone()))
        .ok()
        .flatten();
    let Some(input) = input else { return };

    let target = TARGET.get().copied().unwrap_or("unknown");
    let target = target
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-') { ch } else { '_' })
        .collect::<String>();
    let root = std::env::var_os("ACTORPLANE_FUZZ_ARTIFACT_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("fuzz/artifacts"));
    let directory = root.join(target);

    if fs::create_dir_all(&directory).is_err() {
        return;
    }
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    for attempt in 0..8 {
        let path = directory.join(format!(
            "panic-{}-{}-{}-{}.bin",
            std::process::id(), timestamp, sequence, attempt
        ));
        let Ok(mut file) = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
        else {
            continue;
        };
        if file.write_all(&input).is_ok() && file.flush().is_ok() {
            let mut stderr = std::io::stderr().lock();
            let _ = writeln!(stderr, "actorplane fuzz panic input: {}", path.display());
        }
        return;
    }
}
