#![allow(unused)]
mod consts;
mod dandiapi;
mod paths;
mod s3;
use anyhow::Context;
use axum::{body::Body, extract::Request, Router};
use clap::Parser;
use http::response::Response;
use std::convert::Infallible;
use std::net::IpAddr;
use tower::service_fn;

#[derive(Clone, Debug, Eq, Parser, PartialEq)]
struct Arguments {
    #[arg(long, default_value = "127.0.0.1")]
    ip_addr: IpAddr,

    #[arg(short, long, default_value_t = 8080)]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Arguments::parse();
    let app = Router::new().nest_service("/", service_fn(handle_request));
    let listener = tokio::net::TcpListener::bind((args.ip_addr, args.port))
        .await
        .context("failed to bind listener")?;
    axum::serve(listener, app)
        .await
        .context("failed to serve application")?;
    Ok(())
}

async fn handle_request(_req: Request) -> Result<Response<Body>, Infallible> {
    todo!()
}
