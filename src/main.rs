use chats_openspec::{Config, start};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt().with_target(false).init();
    let config = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    let static_dir = config.static_dir.clone();
    let running = match start(config).await {
        Ok(r) => r,
        Err(e) => {
            eprintln!("failed to start: {e}");
            std::process::exit(1);
        }
    };
    tracing::info!("serving {} on http://{}", static_dir.display(), running.addr);
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down");
    running.stop().await;
}
