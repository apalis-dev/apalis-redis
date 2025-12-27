use std::env;

use apalis::prelude::*;
use apalis_board::axum::sse::{TracingBroadcaster, TracingSubscriber};
use apalis_redis::{RedisConfig, RedisContext, RedisStorage};
use futures::TryFutureExt;
use tokio::signal::ctrl_c;
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    let broadcaster = TracingBroadcaster::create();

    let line_sub = TracingSubscriber::new(&broadcaster);

    let tracer = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_filter(EnvFilter::builder().parse("debug").unwrap()),
        )
        .with(
            line_sub
                .layer()
                .with_filter(EnvFilter::builder().parse("trace").unwrap()),
        );
    tracer.try_init().unwrap();

    let conn = apalis_redis::connect(env::var("REDIS_URL").unwrap())
        .await
        .unwrap();
    let mut backend = RedisStorage::new_with_config(
        conn,
        RedisConfig::default()
            .set_namespace("redis_basic_worker_with_ui")
            .set_buffer_size(100),
    );

    backend.push(42u32).await.unwrap();

    async fn task(task: u32, ctx: RedisContext) -> Result<(), BoxDynError> {
        let handle = std::thread::current();
        tracing::info!("{task:?}, {ctx:?}, Thread: {:?}", handle.id());
        Ok(())
    }

    let worker = WorkerBuilder::new("rango-tango-1")
        .backend(backend.clone())
        .on_event(|ctx, ev| {
            tracing::info!("CTX {:?}, On Event = {:?}", ctx.name(), ev);
        })
        .enable_tracing()
        .build(task)
        .run_until(ctrl_c())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e));

    tokio::select! {
        res = worker => {
            if let Err(e) = res {
                tracing::error!("Worker error: {:?}", e);
            }
        }
        res = http::run_api_server(backend, broadcaster) => {
            if let Err(e) = res {
                tracing::error!("API server error: {:?}", e);
            }
        }
    }
}

mod http {
    use std::sync::{Arc, Mutex};

    use apalis_board::axum::{
        framework::{ApiBuilder, RegisterRoute},
        sse::TracingBroadcaster,
        ui::ServeUI,
    };
    use apalis_redis::RedisStorage;
    use axum::{Extension, Router, ServiceExt};
    use futures::FutureExt;
    use tokio::signal::ctrl_c;
    use tower::Layer;
    use tower_http::normalize_path::NormalizePathLayer;

    pub async fn run_api_server(
        backend: RedisStorage<u32>,
        broadcaster: Arc<Mutex<TracingBroadcaster>>,
    ) -> Result<(), std::io::Error> {
        let api = ApiBuilder::new(Router::new())
            .register(backend.clone())
            .build();
        let layer = NormalizePathLayer::trim_trailing_slash();

        let app = Router::new()
            .nest("/api/v1", api)
            .fallback_service(ServeUI::new())
            .layer(Extension(broadcaster));

        let app = ServiceExt::<axum::extract::Request>::into_make_service(layer.layer(app));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
            .await
            .unwrap();

        axum::serve(listener, app)
            .with_graceful_shutdown(ctrl_c().map(|r| r.unwrap()))
            .await
    }
}
