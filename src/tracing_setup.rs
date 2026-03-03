#[cfg(feature = "tracing-init")]
use std::io::IsTerminal;
#[cfg(feature = "tracing-init")]
use std::sync::OnceLock;

use anyhow::Result;

#[cfg(feature = "tracing-init")]
static INIT: OnceLock<()> = OnceLock::new();

#[cfg(feature = "tracing-init")]
pub fn init_tracing() -> Result<()> {
    if INIT.get().is_some() {
        return Ok(());
    }

    #[cfg(not(feature = "log-compat"))]
    {
        let _ = tracing_log::LogTracer::init();
        tracing_log::log::set_max_level(tracing_log::log::LevelFilter::Trace);
    }

    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::{EnvFilter, Registry};

    let env_filter = EnvFilter::from_default_env();

    let install_result = if std::io::stdout().is_terminal() {
        let fmt_layer = tracing_subscriber::fmt::layer().compact();
        let subscriber = Registry::default().with(env_filter).with(fmt_layer);
        tracing::subscriber::set_global_default(subscriber)
    } else {
        #[cfg(feature = "stackdriver")]
        {
            let stackdriver = tracing_stackdriver::layer();
            let subscriber = Registry::default().with(env_filter).with(stackdriver);
            tracing::subscriber::set_global_default(subscriber)
        }
        #[cfg(not(feature = "stackdriver"))]
        {
            let fmt_layer = tracing_subscriber::fmt::layer().compact();
            let subscriber = Registry::default().with(env_filter).with(fmt_layer);
            tracing::subscriber::set_global_default(subscriber)
        }
    };

    if install_result.is_err() {
        // Global subscriber is likely already installed by the application.
        INIT.get_or_init(|| ());
        return Ok(());
    }

    INIT.get_or_init(|| ());
    tracing::info!("logger initialized");
    Ok(())
}

#[cfg(not(feature = "tracing-init"))]
pub fn init_tracing() -> Result<()> {
    anyhow::bail!("feature `tracing-init` is disabled")
}

#[cfg(all(test, feature = "tracing-init"))]
mod tests {
    use super::init_tracing;

    #[test]
    fn init_is_idempotent() {
        init_tracing().expect("first init should succeed");
        init_tracing().expect("second init should be no-op");
    }
}
