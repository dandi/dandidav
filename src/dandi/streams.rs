use super::types::Page;
use super::{DandiClient, DandiError};
use crate::httputil::{Client, HttpError};
use futures_util::{future::BoxFuture, FutureExt, Stream};
use pin_project::pin_project;
use serde::de::DeserializeOwned;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use url::Url;

// Implementing paginate() as a manually-implemented Stream instead of via
// async_stream lets us save about 4700 bytes on dandidav's top-level Futures.
#[pin_project]
#[must_use = "streams do nothing unless polled"]
pub(super) struct Paginate<T> {
    client: Client,
    state: PaginateState<T>,
}

enum PaginateState<T> {
    Requesting(BoxFuture<'static, Result<Page<T>, HttpError>>),
    Yielding {
        results: std::vec::IntoIter<T>,
        next: Option<Url>,
    },
    Done,
}

impl<T> Paginate<T> {
    pub(super) fn new(client: &DandiClient, url: Url) -> Self {
        Paginate {
            client: client.inner.clone(),
            state: PaginateState::Yielding {
                results: Vec::new().into_iter(),
                next: Some(url),
            },
        }
    }
}

impl<T> Stream for Paginate<T>
where
    T: DeserializeOwned + 'static,
{
    type Item = Result<T, DandiError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        loop {
            match this.state {
                PaginateState::Requesting(ref mut fut) => match ready!(fut.as_mut().poll(cx)) {
                    Ok(page) => {
                        *this.state = PaginateState::Yielding {
                            results: page.results.into_iter(),
                            next: page.next,
                        }
                    }
                    Err(e) => {
                        *this.state = PaginateState::Done;
                        return Some(Err(DandiError::from(e))).into();
                    }
                },
                PaginateState::Yielding {
                    ref mut results,
                    ref mut next,
                } => {
                    if let Some(item) = results.next() {
                        return Some(Ok(item)).into();
                    } else if let Some(url) = next.take() {
                        *this.state =
                            PaginateState::Requesting(this.client.get_json::<Page<T>>(url).boxed());
                    } else {
                        *this.state = PaginateState::Done;
                    }
                }
                PaginateState::Done => return None.into(),
            }
        }
    }
}
