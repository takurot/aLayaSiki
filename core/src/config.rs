use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;
use std::env;

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StorageConfig {
    pub data_dir: String,
    pub wal_flush_interval_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub storage: StorageConfig,
}

impl AppConfig {
    /// Loads configuration with precedence (lowest to highest):
    /// 1. `<config_dir>/default.toml` (checked into the repo at `config/default.toml`,
    ///    required)
    /// 2. `<config_dir>/<RUN_MODE>.toml` (optional; `RUN_MODE` defaults to `development`)
    /// 3. Environment variables prefixed with `ALAYASIKI_`, using `__` as the nested-key
    ///    separator (e.g. `ALAYASIKI_SERVER__PORT`, `ALAYASIKI_STORAGE__DATA_DIR`).
    ///
    /// `config_dir` defaults to `config` (relative to the process working directory) and
    /// can be overridden via the `ALAYASIKI_CONFIG_DIR` environment variable, e.g. to point
    /// at an absolute path when the process is not started from the repository root.
    pub fn load() -> Result<Self, ConfigError> {
        let run_mode = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());
        let config_dir = env::var("ALAYASIKI_CONFIG_DIR").unwrap_or_else(|_| "config".into());

        let builder = Config::builder()
            .add_source(File::with_name(&format!("{config_dir}/default")))
            .add_source(File::with_name(&format!("{config_dir}/{run_mode}")).required(false))
            .add_source(
                Environment::with_prefix("ALAYASIKI")
                    .prefix_separator("_")
                    .separator("__")
                    .try_parsing(true),
            );

        builder.build()?.try_deserialize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::sync::Mutex;

    // Guards mutation of process-global environment variables across tests.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        vars: Vec<String>,
        _lock: std::sync::MutexGuard<'static, ()>,
    }

    impl EnvGuard {
        fn new() -> Self {
            let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            Self {
                vars: Vec::new(),
                _lock: lock,
            }
        }

        fn set(&mut self, key: &str, value: &str) {
            env::set_var(key, value);
            self.vars.push(key.to_string());
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for key in &self.vars {
                env::remove_var(key);
            }
        }
    }

    fn repo_config_dir() -> String {
        // core/'s manifest dir is `<repo_root>/core`; the checked-in defaults live in
        // `<repo_root>/config`.
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("core crate has a parent directory")
            .join("config")
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn loads_checked_in_defaults_from_repo_root() {
        let mut guard = EnvGuard::new();
        guard.set("ALAYASIKI_CONFIG_DIR", &repo_config_dir());
        guard.set("RUN_MODE", "nonexistent-run-mode");

        let config = AppConfig::load().expect("load should succeed with checked-in defaults");

        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.storage.data_dir, "./data");
        assert_eq!(config.storage.wal_flush_interval_ms, 100);
    }

    #[test]
    fn missing_optional_run_mode_file_falls_back_to_defaults() {
        let dir = tempfile::tempdir().expect("create temp dir");
        std::fs::write(
            dir.path().join("default.toml"),
            "[server]\nhost = \"0.0.0.0\"\nport = 1234\n\n[storage]\ndata_dir = \"/tmp/data\"\nwal_flush_interval_ms = 50\n",
        )
        .expect("write default.toml");

        let mut guard = EnvGuard::new();
        guard.set("ALAYASIKI_CONFIG_DIR", &dir.path().to_string_lossy());
        guard.set("RUN_MODE", "does-not-exist");

        let config = AppConfig::load().expect("missing run-mode file must not be an error");

        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 1234);
    }

    #[test]
    fn environment_variables_override_file_values() {
        let dir = tempfile::tempdir().expect("create temp dir");
        std::fs::write(
            dir.path().join("default.toml"),
            "[server]\nhost = \"0.0.0.0\"\nport = 1234\n\n[storage]\ndata_dir = \"/tmp/data\"\nwal_flush_interval_ms = 50\n",
        )
        .expect("write default.toml");

        let mut guard = EnvGuard::new();
        guard.set("ALAYASIKI_CONFIG_DIR", &dir.path().to_string_lossy());
        guard.set("RUN_MODE", "does-not-exist");
        guard.set("ALAYASIKI_SERVER__PORT", "9999");
        guard.set("ALAYASIKI_STORAGE__DATA_DIR", "/var/lib/alayasiki");

        let config = AppConfig::load().expect("load with env overrides should succeed");

        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 9999);
        assert_eq!(config.storage.data_dir, "/var/lib/alayasiki");
        assert_eq!(config.storage.wal_flush_interval_ms, 50);
    }

    #[test]
    fn missing_required_default_file_is_an_error() {
        let dir = tempfile::tempdir().expect("create temp dir");

        let mut guard = EnvGuard::new();
        guard.set("ALAYASIKI_CONFIG_DIR", &dir.path().to_string_lossy());

        assert!(AppConfig::load().is_err());
    }
}
