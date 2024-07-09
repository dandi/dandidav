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

#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub(super) struct ListEntryPages<'a> {
    bucket: &'a CompactString,
    key_prefix: &'a str,
    inner: Option<PaginationStream<Result<ListObjectsV2Output, ListObjectsError>>>,
}

impl<'a> ListEntryPages<'a> {
    pub(super) fn new(client: &'a S3Client, key_prefix: &'a str) -> Self {
        ListEntryPages {
            bucket: &client.bucket,
            key_prefix,
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
            prefix: self.key_prefix.to_owned(),
            source,
        })
    }

    fn die_bad_object<T>(
        &mut self,
        source: TryFromAwsObjectError,
    ) -> Poll<Option<Result<T, S3Error>>> {
        self.die(S3Error::BadObject {
            bucket: self.bucket.clone(),
            prefix: self.key_prefix.to_owned(),
            source,
        })
    }

    fn die_bad_prefix<T>(
        &mut self,
        source: TryFromCommonPrefixError,
    ) -> Poll<Option<Result<T, S3Error>>> {
        self.die(S3Error::BadPrefix {
            bucket: self.bucket.clone(),
            prefix: self.key_prefix.to_owned(),
            source,
        })
    }
}

impl Stream for ListEntryPages<'_> {
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
            .map(|obj| S3Object::try_from_aws_object(obj, self.bucket))
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
