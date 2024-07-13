//! Extensions for stream types
use futures_util::{Stream, TryStream};
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

/// Extension methods for [`futures_util::TryStream`]
pub(crate) trait TryStreamUtil: TryStream {
    /// Wraps the current stream in a new stream that maps the success values
    /// through `f` to produce an iterator; the success values of the new
    /// stream will then be the elements of the concatenation of those
    /// iterators.
    fn try_flat_iter_map<I, F>(self, f: F) -> TryFlatIterMap<Self, I, F>
    where
        F: FnMut(Self::Ok) -> I,
        I: IntoIterator,
        Self: Sized,
    {
        TryFlatIterMap::new(self, f)
    }
}

impl<S: TryStream> TryStreamUtil for S {}

pin_project! {
    /// Return type of [`TryStreamUtil::try_flat_iter_map()`]
    #[derive(Clone, Debug)]
    #[must_use = "streams do nothing unless polled"]
    pub(crate) struct TryFlatIterMap<S, I: IntoIterator, F> {
        #[pin]
        inner: S,
        f: F,
        iter: Option<I::IntoIter>,
    }
}

impl<S, I: IntoIterator, F> TryFlatIterMap<S, I, F> {
    /// Construct a `TryFlatIterMap` for a call to `inner.try_flat_iter_map(f)`
    fn new(inner: S, f: F) -> Self {
        TryFlatIterMap {
            inner,
            f,
            iter: None,
        }
    }
}

impl<S, I, F> Stream for TryFlatIterMap<S, I, F>
where
    S: TryStream,
    F: FnMut(S::Ok) -> I,
    I: IntoIterator,
{
    type Item = Result<I::Item, S::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(iter) = this.iter.as_mut() {
                if let Some(r) = iter.next() {
                    return Some(Ok(r)).into();
                } else {
                    *this.iter = None;
                }
            }
            match ready!(this.inner.as_mut().try_poll_next(cx)) {
                Some(Ok(iter)) => *this.iter = Some((this.f)(iter).into_iter()),
                Some(Err(e)) => return Some(Err(e)).into(),
                None => return None.into(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt;

    #[tokio::test]
    async fn test_try_flat_iter_map() {
        let mut stream = futures_util::stream::iter(vec![Ok(5), Ok(2), Err(42), Ok(3)])
            .try_flat_iter_map(|x| 0..x);
        assert_eq!(stream.try_next().await, Ok(Some(0)));
        assert_eq!(stream.try_next().await, Ok(Some(1)));
        assert_eq!(stream.try_next().await, Ok(Some(2)));
        assert_eq!(stream.try_next().await, Ok(Some(3)));
        assert_eq!(stream.try_next().await, Ok(Some(4)));
        assert_eq!(stream.try_next().await, Ok(Some(0)));
        assert_eq!(stream.try_next().await, Ok(Some(1)));
        assert_eq!(stream.try_next().await, Err(42));
        assert_eq!(stream.try_next().await, Ok(Some(0)));
        assert_eq!(stream.try_next().await, Ok(Some(1)));
        assert_eq!(stream.try_next().await, Ok(Some(2)));
        assert_eq!(stream.try_next().await, Ok(None));
    }
}
