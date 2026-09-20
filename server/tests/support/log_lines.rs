//! The log files a field report is read from, as a test reads them back.
//!
//! Some failures leave nothing behind but one INFO line -- what a playback
//! delivered, and what ended it -- so the tests that pin those lines assert
//! over the files the process actually writes (`server_<launch>.jsonl`)
//! rather than over a function that writes them. This is the reading half
//! of that, shared by `#[path]` between the binaries which do it: one per
//! subscriber, because `init_logging` installs the process's once and a
//! second such test in the same binary would write into whichever tempdir
//! won the race.
//!
//! Which is also why nothing here is `#[cfg(test)]` and why the
//! unused-function lint is off: each includer uses the part of it its own
//! subject needs.

#![allow(dead_code)]

use std::time::{Duration, Instant};

/// How long a line is given to reach the file. The log writer is not
/// blocking, so a line is written some short while after the event it
/// describes -- and this is a bound on a mistake, not a wait anything
/// normally spends.
const LINE_WAIT_BOUND: Duration = Duration::from_secs(20);

/// Every line the process has written at `stage`, as the JSON log carries
/// it.
pub fn lines_at_stage(config_dir: &std::path::Path, stage: &str) -> Vec<serde_json::Value> {
    let mut lines = vec![];
    let Ok(dir) = std::fs::read_dir(config_dir.join("logs")) else {
        return lines;
    };
    for entry in dir.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("jsonl") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in text.lines() {
            let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                continue;
            };
            if value["fields"]["stage"] == stage {
                lines.push(value);
            }
        }
    }
    lines
}

/// The line at `stage` that `wanted` describes, waited for; `described` is
/// what the failure calls it when none arrives.
///
/// `wanted` is handed the line's fields, which is where everything a line
/// states lives.
pub fn wait_for_line(
    config_dir: &std::path::Path,
    stage: &str,
    described: &str,
    wanted: impl Fn(&serde_json::Value) -> bool,
) -> anyhow::Result<serde_json::Value> {
    let deadline = Instant::now() + LINE_WAIT_BOUND;
    loop {
        let lines = lines_at_stage(config_dir, stage);
        if let Some(found) = lines.iter().find(|line| wanted(&line["fields"])) {
            return Ok(found.clone());
        }
        if Instant::now() >= deadline {
            anyhow::bail!("no {stage} line for {described} in {lines:#?}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}
