#[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
mod app {
    use fslock::LockFile;

    #[global_allocator]
    static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

    pub fn main() -> anyhow::Result<()> {
        let lock_path = std::env::temp_dir().join("stream-server.lock");
        let mut lockfile = LockFile::open(&lock_path)?;

        if !lockfile.try_lock()? {
            eprintln!("Exiting, another instance is running.");
            return Ok(());
        }

        let args: Vec<String> = std::env::args().collect();
        let use_tui = args.iter().any(|a| a == "--tui");

        if let Err(err) = run(use_tui) {
            if let Some(missing) = err.downcast_ref::<stream_server::MissingFfmpegError>() {
                eprintln!("{}", missing.details());
                eprintln!(
                    "Install FFmpeg, make sure both ffmpeg and ffprobe are available in PATH, then start Stream Server again."
                );
                std::process::exit(1);
            }
            return Err(err);
        }

        Ok(())
    }

    fn run(use_tui: bool) -> anyhow::Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut cfg = stream_server::ServerConfig::binary_default();
        cfg.use_tui = use_tui;
        let _ = rt.block_on(stream_server::run(cfg, rx, None))?;
        Ok(())
    }
}

#[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
fn main() -> anyhow::Result<()> {
    app::main()
}

#[cfg(not(any(target_os = "windows", target_os = "macos", target_os = "linux")))]
fn main() -> anyhow::Result<()> {
    Ok(())
}
