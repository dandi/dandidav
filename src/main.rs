#[macro_use]
mod validstr;

mod consts;
mod dandi;
mod dav;
mod httputil;
mod paths;
mod s3;
mod streamutil;
mod tests;
mod zarrman;
use crate::consts::*;
use crate::dandi::DandiClient;
use crate::dav::{DandiDav, Templater};
use crate::httputil::HttpUrl;
use crate::zarrman::{ManifestFetcher, ZarrManClient};
use anyhow::Context;
use axum::{
    body::Body,
    error_handling::HandleErrorLayer,
    extract::Request,
    http::{
        header::{
            HeaderValue, ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_LENGTH, CONTENT_TYPE, SERVER,
            USER_AGENT,
        },
        response::Response,
        Method, StatusCode,
    },
    middleware::{self, Next},
    routing::get,
    Router,
};
use clap::{Args, Parser};
use http_body::Body as _;
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;
use tower::{service_fn, ServiceBuilder};
use tower_governor::{
    governor::GovernorConfigBuilder, key_extractor::SmartIpKeyExtractor, GovernorLayer,
};
use tower_http::{set_header::response::SetResponseHeaderLayer, trace::TraceLayer};
use tracing::Level;
use tracing_subscriber::{filter::Targets, fmt::time::OffsetTime, prelude::*};

/// The content of the CSS stylesheet to serve at `/.static/styles.css`
static STYLESHEET: &str = include_str!("dav/static/styles.css");

/// The content of the `robots.txt` file to serve at `/robots.txt`
static ROBOTS_TXT: &str = "User-agent: *\nDisallow: /\n";

/// The body to return with 408 Request Timeout responses
static REQUEST_TIMEOUT_BODY: &str = "Request could not be completed in time\n";

/// WebDAV view to DANDI Archive
///
/// See <https://github.com/dandi/dandidav> for more information.
#[derive(Clone, Debug, Eq, Parser, PartialEq)]
#[command(version = env!("VERSION_WITH_GIT"))]
struct Arguments {
    #[command(flatten)]
    config: Config,

    /// IP address to listen on
    #[arg(long, default_value = "127.0.0.1")]
    ip_addr: IpAddr,

    /// Port to listen on
    #[arg(short, long, default_value_t = 8080)]
    port: u16,
}

#[derive(Args, Clone, Debug, Eq, PartialEq)]
struct Config {
    /// API URL of the DANDI Archive instance to serve
    #[arg(long, default_value = DEFAULT_API_URL, value_name = "URL")]
    api_url: HttpUrl,

    /// Log the process's memory usage at the start & end of each incoming
    /// request
    #[arg(long)]
    log_memory: bool,

    /// Redirect requests for blob assets directly to S3 instead of to Archive
    /// URLs that redirect to signed S3 URLs
    #[arg(long)]
    prefer_s3_redirects: bool,

    /// Site name to use in HTML collection pages
    #[arg(short = 'T', long, default_value = env!("CARGO_PKG_NAME"))]
    title: String,

    /// Limit the Zarr manifest cache to storing no more than this many
    /// megabytes of parsed manifests at once
    #[arg(short = 'Z', long, default_value_t = 100, value_name = "INT")]
    zarrman_cache_mb: u64,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            api_url: DEFAULT_API_URL
                .parse::<HttpUrl>()
                .expect("DEFAULT_API_URL should be a valid HttpUrl"),
            log_memory: false,
            prefer_s3_redirects: false,
            title: env!("CARGO_PKG_NAME").into(),
            zarrman_cache_mb: 100,
        }
    }
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
                .json()
                .with_timer(timer)
                .with_writer(std::io::stderr),
        )
        .with(
            Targets::new()
                .with_target(env!("CARGO_CRATE_NAME"), Level::TRACE)
                .with_target("aws_config", Level::DEBUG)
                .with_target("reqwest", Level::TRACE)
                .with_target("reqwest_retry", Level::TRACE)
                .with_target("tower_http", Level::TRACE)
                .with_default(Level::INFO),
        )
        .init();
    run()
}

#[tokio::main]
async fn run() -> anyhow::Result<()> {
    let args = Arguments::parse();
    let app = get_app(args.config)?;
    let listener = tokio::net::TcpListener::bind((args.ip_addr, args.port))
        .await
        .context("failed to bind listener")?;
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await
    .context("failed to serve application")?;
    Ok(())
}

fn get_app(cfg: Config) -> anyhow::Result<Router> {
    let dandi = DandiClient::new(cfg.api_url)?;
    let zarrfetcher = ManifestFetcher::new(cfg.zarrman_cache_mb * 1_000_000)?;
    zarrfetcher.install_periodic_dump(ZARR_MANIFEST_CACHE_DUMP_PERIOD);
    let zarrman = ZarrManClient::new(zarrfetcher);
    let templater = Templater::new(cfg.title)?;
    let dav = Arc::new(DandiDav {
        dandi,
        zarrman,
        templater,
        prefer_s3_redirects: cfg.prefer_s3_redirects,
    });
    let mut app = Router::new()
        .route(
            "/.static/styles.css",
            get(|| async {
                // Note: This response should not have WebDAV headers (DAV, Allow)
                ([(CONTENT_TYPE, CSS_CONTENT_TYPE)], STYLESHEET)
            }),
        )
        .route(
            "/robots.txt",
            get(|| async {
                // Note: This response should not have WebDAV headers (DAV, Allow)
                ([(CONTENT_TYPE, CSS_CONTENT_TYPE)], ROBOTS_TXT)
            }),
        )
        .fallback_service(service_fn(move |req: Request| {
            let dav = Arc::clone(&dav);
            async move { dav.handle_request(req).await }
        }))
        .layer(middleware::from_fn(handle_head));
    if cfg.log_memory {
        app = app.layer(middleware::from_fn(log_memory));
    }
    app = app
        .layer(SetResponseHeaderLayer::if_not_present(
            SERVER,
            HeaderValue::from_static(SERVER_VALUE),
        ))
        .layer(SetResponseHeaderLayer::if_not_present(
            ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("*"),
        ))
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|_| async { (StatusCode::REQUEST_TIMEOUT, REQUEST_TIMEOUT_BODY) }))
                .timeout(std::time::Duration::from_secs(25))
        )
        .layer(GovernorLayer {
            config: Arc::new(
                GovernorConfigBuilder::default()
                    .key_extractor(SmartIpKeyExtractor)
                    .finish()
                    .expect("building GovernorConfig should not fail"),
            ),
        })
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request<_>| {
                    tracing::debug_span!(
                        "request",
                        method = %request.method(),
                        uri = %request.uri(),
                        request_id = request.headers().get("X-Request-ID").and_then(|v| v.to_str().ok()),
                    )
                })
                .on_request(|request: &Request<_>, _span: &tracing::span::Span| {
                    tracing::debug!(
                        user_agent = request.headers().get(USER_AGENT).and_then(|v| v.to_str().ok()),
                        "starting processing request",
                    );
                }),
        );
    Ok(app)
}

/// Handle `HEAD` requests by converting them to `GET` requests and discarding
/// the resulting response body
async fn handle_head(method: Method, mut request: Request<Body>, next: Next) -> Response<Body> {
    if method == Method::HEAD {
        *request.method_mut() = Method::GET;
        let mut resp = next.run(request).await;
        if let Some(sz) = resp.body().size_hint().exact() {
            resp.headers_mut().insert(CONTENT_LENGTH, sz.into());
        }
        *resp.body_mut() = Body::empty();
        resp
    } else {
        next.run(request).await
    }
}

async fn log_memory(request: Request<Body>, next: Next) -> Response<Body> {
    fn getmem(rel: &str) -> Option<memory_stats::MemoryStats> {
        if let Some(stats) = memory_stats::memory_stats() {
            tracing::info!(
                "Memory usage {} request: {} physical, {} virtual",
                rel,
                stats.physical_mem,
                stats.virtual_mem,
            );
            Some(stats)
        } else {
            tracing::info!("Failed to get memory usage {rel} request");
            None
        }
    }

    let mem_before = getmem("before");
    let r = next.run(request).await;
    let mem_after = getmem("after");
    if let Some((before, after)) = mem_before.zip(mem_after) {
        tracing::info!(
            "Change in memory usage: physical {}, virtual {}",
            UsizeDiff::new(before.physical_mem, after.physical_mem),
            UsizeDiff::new(before.virtual_mem, after.virtual_mem),
        );
    } else {
        tracing::info!("Change in memory usage could not be computed");
    }
    r
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct UsizeDiff {
    before: usize,
    after: usize,
}

impl UsizeDiff {
    fn new(before: usize, after: usize) -> UsizeDiff {
        UsizeDiff { before, after }
    }
}

impl fmt::Display for UsizeDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}",
            if self.after < self.before { '-' } else { '+' },
            self.before.abs_diff(self.after)
        )
    }
}
