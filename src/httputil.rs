use crate::consts::USER_AGENT;
use reqwest::ClientBuilder;
use thiserror::Error;

pub(crate) fn new_client() -> Result<reqwest::Client, BuildClientError> {
    ClientBuilder::new()
        .user_agent(USER_AGENT)
        .build()
        .map_err(Into::into)
}

#[derive(Debug, Error)]
#[error("failed to initialize HTTP client")]
pub(crate) struct BuildClientError(#[from] reqwest::Error);
