#[cfg(any(target_os = "windows", target_os = "macos", target_os = "linux"))]
mod app {
    use fslock::LockFile;
    use stream_server::ServerAuth;

    #[global_allocator]
    static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

    /// Environment variable read for the control API token when `--token`
    /// is not given (headless deployments that must not parse stdout).
    pub(super) const TOKEN_ENV: &str = "STREAM_SERVER_TOKEN";

    #[derive(Debug, PartialEq, Eq)]
    pub(super) struct CliOptions {
        pub use_tui: bool,
        pub auth: ServerAuth,
    }

    /// Parse the daemon's command line. `--tui` selects the ratatui mode.
    /// The control API token comes from, in order of precedence: `--no-auth`
    /// (every route open), `--token <t>` / `--token=<t>`, the
    /// `STREAM_SERVER_TOKEN` environment variable (`env_token`; blank counts
    /// as unset), else a fresh per-launch token. `--no-auth` next to an
    /// explicit `--token` is a contradiction and rejected; next to the
    /// environment variable the explicit flag wins. Unknown arguments are
    /// ignored, as they always were.
    pub(super) fn parse_cli(
        args: impl IntoIterator<Item = String>,
        env_token: Option<String>,
    ) -> anyhow::Result<CliOptions> {
        let mut use_tui = false;
        let mut no_auth = false;
        let mut token: Option<String> = None;
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--tui" => use_tui = true,
                "--no-auth" => no_auth = true,
                "--token" => {
                    let value = args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--token requires a value"))?;
                    token = Some(value);
                }
                _ => {
                    if let Some(value) = arg.strip_prefix("--token=") {
                        token = Some(value.to_string());
                    }
                }
            }
        }
        if let Some(token) = &token {
            anyhow::ensure!(!token.is_empty(), "--token must not be empty");
            anyhow::ensure!(!no_auth, "--no-auth and --token contradict each other");
        }
        let env_token = env_token.filter(|value| !value.trim().is_empty());
        let auth = if no_auth {
            ServerAuth::Disabled
        } else if let Some(token) = token.or(env_token) {
            ServerAuth::Token(token)
        } else {
            ServerAuth::Generated
        };
        Ok(CliOptions { use_tui, auth })
    }

    pub fn main() -> anyhow::Result<()> {
        let lock_path = std::env::temp_dir().join("stream-server.lock");
        let mut lockfile = LockFile::open(&lock_path)?;

        if !lockfile.try_lock()? {
            eprintln!("Exiting, another instance is running.");
            return Ok(());
        }

        let options = parse_cli(std::env::args().skip(1), std::env::var(TOKEN_ENV).ok())?;
        run(options)?;

        Ok(())
    }

    fn run(options: CliOptions) -> anyhow::Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        let mut cfg = stream_server::ServerConfig::binary_default();
        cfg.use_tui = options.use_tui;
        cfg.auth = options.auth;
        let _ = rt.block_on(stream_server::run(cfg, rx, None))?;
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn parse(args: &[&str], env_token: Option<&str>) -> anyhow::Result<CliOptions> {
            parse_cli(
                args.iter().map(|arg| arg.to_string()),
                env_token.map(str::to_string),
            )
        }

        #[test]
        fn defaults_to_a_generated_token_without_tui() {
            let options = parse(&[], None).unwrap();
            assert_eq!(
                options,
                CliOptions {
                    use_tui: false,
                    auth: ServerAuth::Generated,
                }
            );
            // Unknown arguments are ignored, as before.
            assert_eq!(parse(&["--verbose"], None).unwrap(), options);
        }

        #[test]
        fn tui_and_no_auth_flags() {
            let options = parse(&["--tui", "--no-auth"], None).unwrap();
            assert!(options.use_tui);
            assert_eq!(options.auth, ServerAuth::Disabled);
        }

        #[test]
        fn token_flag_in_both_spellings() {
            assert_eq!(
                parse(&["--token", "abc"], None).unwrap().auth,
                ServerAuth::Token("abc".into())
            );
            assert_eq!(
                parse(&["--token=abc"], None).unwrap().auth,
                ServerAuth::Token("abc".into())
            );
        }

        #[test]
        fn token_flag_beats_the_environment_which_beats_generated() {
            assert_eq!(
                parse(&[], Some("from-env")).unwrap().auth,
                ServerAuth::Token("from-env".into())
            );
            assert_eq!(
                parse(&["--token", "flag"], Some("from-env")).unwrap().auth,
                ServerAuth::Token("flag".into())
            );
            // A blank variable counts as unset.
            assert_eq!(parse(&[], Some("  ")).unwrap().auth, ServerAuth::Generated);
        }

        #[test]
        fn no_auth_beats_the_environment_but_contradicts_an_explicit_token() {
            assert_eq!(
                parse(&["--no-auth"], Some("from-env")).unwrap().auth,
                ServerAuth::Disabled
            );
            assert!(parse(&["--no-auth", "--token", "abc"], None).is_err());
            assert!(parse(&["--token=abc", "--no-auth"], None).is_err());
        }

        #[test]
        fn token_flag_rejects_missing_and_empty_values() {
            assert!(parse(&["--token"], None).is_err());
            assert!(parse(&["--token", ""], None).is_err());
            assert!(parse(&["--token="], None).is_err());
        }
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
