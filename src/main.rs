mod consts;
mod dandi;
mod dav;
mod paths;
mod s3;
use crate::consts::DEFAULT_API_URL;
use crate::dandi::Client;
use crate::dav::{DandiDav, Templater};
use anyhow::Context;
use axum::{
    body::Body,
    extract::Request,
    http::{response::Response, Method},
    middleware::{self, Next},
    response::IntoResponse,
    Router,
};
use clap::Parser;
use std::convert::Infallible;
use std::net::IpAddr;
use std::sync::Arc;
use tower::service_fn;
use tower_http::trace::TraceLayer;
use tracing_subscriber::filter::LevelFilter;

/// WebDAV view to DANDI Archive
///
/// See <https://github.com/jwodder/dandidav> for more information.
#[derive(Clone, Debug, Eq, Parser, PartialEq)]
#[command(version = env!("VERSION_WITH_GIT"))]
struct Arguments {
    /// API URL of the DANDI Archive instance to serve
    #[arg(long, default_value = DEFAULT_API_URL, value_name = "URL")]
    api_url: url::Url,

    /// IP address to listen on
    #[arg(long, default_value = "127.0.0.1")]
    ip_addr: IpAddr,

    /// Port to listen on
    #[arg(short, long, default_value_t = 8080)]
    port: u16,

    /// Site name to include in HTML page titles
    #[arg(short = 'T', long, default_value = env!("CARGO_PKG_NAME"))]
    title: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Arguments::parse();
    tracing_subscriber::fmt()
        .with_max_level(LevelFilter::TRACE)
        .init();
    let client = Client::new(args.api_url)?;
    let templater = Templater::load()?;
    let dav = Arc::new(DandiDav::new(client, templater, args.title));
    let app = Router::new()
        .nest_service(
            "/",
            service_fn(move |req: Request| {
                let dav = Arc::clone(&dav);
                async move { Ok::<_, Infallible>(dav.handle_request(req).await.into_response()) }
            }),
        )
        .layer(middleware::from_fn(handle_head))
        .layer(TraceLayer::new_for_http());
    let listener = tokio::net::TcpListener::bind((args.ip_addr, args.port))
        .await
        .context("failed to bind listener")?;
    axum::serve(listener, app)
        .await
        .context("failed to serve application")?;
    Ok(())
}

async fn handle_head(method: Method, mut request: Request<Body>, next: Next) -> Response<Body> {
    if method == Method::HEAD {
        *request.method_mut() = Method::GET;
        let mut resp = next.run(request).await;
        *resp.body_mut() = Body::empty();
        resp
    } else {
        next.run(request).await
    }
}
