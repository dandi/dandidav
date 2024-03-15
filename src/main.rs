#[macro_use]
mod validstr;

mod consts;
mod dandi;
mod dav;
mod httputil;
mod paths;
mod s3;
mod zarrman;
use crate::consts::{CSS_CONTENT_TYPE, DEFAULT_API_URL, SERVER_VALUE};
use crate::dandi::DandiClient;
use crate::dav::{DandiDav, Templater};
use crate::zarrman::ZarrManClient;
use anyhow::Context;
use axum::{
    body::Body,
    extract::Request,
    http::{
        header::{HeaderValue, ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE, SERVER},
        response::Response,
        Method,
    },
    middleware::{self, Next},
    routing::get,
    Router,
};
use clap::Parser;
use std::io::{stderr, IsTerminal};
use std::net::IpAddr;
use std::sync::Arc;
use tower::service_fn;
use tower_http::{set_header::response::SetResponseHeaderLayer, trace::TraceLayer};
use tracing::Level;
use tracing_subscriber::{filter::Targets, fmt::time::OffsetTime, prelude::*};

static STYLESHEET: &str = include_str!("dav/static/styles.css");

/// WebDAV view to DANDI Archive
///
/// See <https://github.com/dandi/dandidav> for more information.
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

    /// Redirect requests for blob assets directly to S3 instead of to Archive
    /// URLs that redirect to signed S3 URLs
    #[arg(long)]
    prefer_s3_redirects: bool,

    /// Site name to use in HTML collection pages
    #[arg(short = 'T', long, default_value = env!("CARGO_PKG_NAME"))]
    title: String,
}

// See
// <https://docs.rs/tracing-subscriber/latest/tracing_subscriber/fmt/time/struct.OffsetTime.html#method.local_rfc_3339>
// for an explanation of the main + #[tokio::main]run thing
fn main() -> anyhow::Result<()> {
    let timer =
        OffsetTime::local_rfc_3339().context("failed to determine local timezone offset")?;
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_timer(timer)
                .with_ansi(stderr().is_terminal())
                .with_writer(stderr),
        )
        .with(
            Targets::new()
                .with_target(env!("CARGO_CRATE_NAME"), Level::TRACE)
                .with_target("aws_config", Level::DEBUG)
                .with_target("reqwest", Level::TRACE)
                .with_target("tower_http", Level::TRACE)
                .with_default(Level::INFO),
        )
        .init();
    run()
}

#[tokio::main]
async fn run() -> anyhow::Result<()> {
    let args = Arguments::parse();
    let dandi = DandiClient::new(args.api_url)?;
    let zarrman = ZarrManClient::new()?;
    let templater = Templater::load()?;
    let dav = Arc::new(DandiDav {
        dandi,
        zarrman,
        templater,
        title: args.title,
        prefer_s3_redirects: args.prefer_s3_redirects,
    });
    let app = Router::new()
        .route(
            "/.static/styles.css",
            get(|| async {
                // Note: This response should not have WebDAV headers (DAV, Allow)
                ([(CONTENT_TYPE, CSS_CONTENT_TYPE)], STYLESHEET)
            }),
        )
        .nest_service(
            "/",
            service_fn(move |req: Request| {
                let dav = Arc::clone(&dav);
                // Box the large future:
                async move { Box::pin(dav.handle_request(req)).await }
            }),
        )
        .layer(middleware::from_fn(handle_head))
        .layer(SetResponseHeaderLayer::if_not_present(
            SERVER,
            HeaderValue::from_static(SERVER_VALUE),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("*"),
        ))
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
