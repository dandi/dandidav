use futures_util::{Stream, TryStream};
use pin_project_lite::pin_project;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

pub(crate) trait TryStreamUtil: TryStream {
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
