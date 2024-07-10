use super::{
    ListObjectsError, S3Client, S3EntryPage, S3Error, S3Folder, S3Object, TryFromAwsObjectError,
    TryFromCommonPrefixError,
};
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_smithy_async::future::pagination_stream::PaginationStream;
use futures_util::Stream;
use smartstring::alias::CompactString;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

// Implementing list_entry_pages() as a manually-implemented Stream instead of
// via async_stream lets us save about 3500 bytes on dandidav's top-level
// Futures.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub(super) struct ListEntryPages {
    bucket: CompactString,
    key_prefix: String,
    inner: Option<PaginationStream<Result<ListObjectsV2Output, ListObjectsError>>>,
}

impl ListEntryPages {
    pub(super) fn new<S: Into<String>>(client: &S3Client, key_prefix: S) -> Self {
        let key_prefix = key_prefix.into();
        ListEntryPages {
            bucket: client.bucket.clone(),
            key_prefix: key_prefix.clone(),
            inner: Some(
                client
                    .inner
                    .list_objects_v2()
                    .bucket(&*client.bucket)
                    .prefix(key_prefix)
                    .delimiter("/")
                    .into_paginator()
                    .send(),
            ),
        }
    }

    fn die<T>(&mut self, e: S3Error) -> Poll<Option<Result<T, S3Error>>> {
        self.inner = None;
        Some(Err(e)).into()
    }

    fn die_list_objects<T>(
        &mut self,
        source: ListObjectsError,
    ) -> Poll<Option<Result<T, S3Error>>> {
        self.die(S3Error::ListObjects {
            bucket: self.bucket.clone(),
            prefix: self.key_prefix.clone(),
            source,
        })
    }

    fn die_bad_object<T>(
        &mut self,
        source: TryFromAwsObjectError,
    ) -> Poll<Option<Result<T, S3Error>>> {
        self.die(S3Error::BadObject {
            bucket: self.bucket.clone(),
            prefix: self.key_prefix.clone(),
            source,
        })
    }

    fn die_bad_prefix<T>(
        &mut self,
        source: TryFromCommonPrefixError,
    ) -> Poll<Option<Result<T, S3Error>>> {
        self.die(S3Error::BadPrefix {
            bucket: self.bucket.clone(),
            prefix: self.key_prefix.clone(),
            source,
        })
    }
}

impl Stream for ListEntryPages {
    type Item = Result<S3EntryPage, S3Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(inner) = self.inner.as_mut() else {
            return None.into();
        };
        let Some(r) = ready!(inner.poll_next(cx)) else {
            self.inner = None;
            return None.into();
        };
        let page = match r {
            Ok(page) => page,
            Err(source) => return self.die_list_objects(source),
        };
        let objects = match page
            .contents
            .unwrap_or_default()
            .into_iter()
            .map(|obj| S3Object::try_from_aws_object(obj, &self.bucket))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(objects) => objects,
            Err(source) => return self.die_bad_object(source),
        };
        let folders = match page
            .common_prefixes
            .unwrap_or_default()
            .into_iter()
            .map(S3Folder::try_from)
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(folders) => folders,
            Err(source) => return self.die_bad_prefix(source),
        };
        Some(Ok(S3EntryPage { folders, objects })).into()
    }
}
