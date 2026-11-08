pub mod api;
pub mod app;
pub mod auth;
pub mod config;
pub mod db;
pub mod error;
pub mod hub;
pub mod model;
pub mod ws;

use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub use app::App;
pub use config::Config;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub struct Running {
    pub addr: SocketAddr,
    pub app: App,
    shutdown: oneshot::Sender<()>,
    server: JoinHandle<std::io::Result<()>>,
    tasks: Vec<JoinHandle<()>>,
}

/// Opens the databases, binds the listener and starts serving.
pub async fn start(config: Config) -> Result<Running, BoxError> {
    let app = App::open(&config).await?;
    let mut tasks = app.hub.spawn_tasks();
    let cleanup_app = app.clone();
    tasks.push(tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            cleanup_app.auth.cleanup(db::now_s());
        }
    }));

    let listener = TcpListener::bind(config.addr).await?;
    let addr = listener.local_addr()?;
    let router = api::router(app.clone(), &config.static_dir);
    let (shutdown, rx) = oneshot::channel::<()>();
    let server = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await
    });
    Ok(Running { addr, app, shutdown, server, tasks })
}

impl Running {
    /// Stops accepting requests, then drains and joins the database writers.
    pub async fn stop(self) {
        let _ = self.shutdown.send(());
        let _ = tokio::time::timeout(Duration::from_secs(5), self.server).await;
        for t in &self.tasks {
            t.abort();
        }
        self.app.db.stop().await;
    }
}
