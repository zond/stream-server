fn main() {
    if let Ok(output) = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        && output.status.success()
    {
        let sha = String::from_utf8_lossy(&output.stdout);
        println!("cargo:rustc-env=GIT_SHA={}", sha.trim());
    }
}
