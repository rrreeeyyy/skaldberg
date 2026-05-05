//! S3 storage backend for Iceberg, built on aws-sdk-s3 directly.
//!
//! `iceberg-rust 0.9` ships an OpenDAL-based S3 storage in
//! `iceberg-storage-opendal`. OpenDAL signs requests with `reqsign`,
//! which has its own credential chain distinct from the AWS SDK and
//! does not understand SSO / shared-profile out of the box. Plumbing
//! the SDK chain into OpenDAL needed a bridge + a custom credential
//! cache, which was complexity we didn't want to own.
//!
//! Going aws-sdk-s3 directly:
//!   - one credential code path (the SDK's own chain — env / SSO /
//!     shared profile / IRSA / IMDS, all with built-in caching and
//!     refresh handled transparently by the SDK client),
//!   - no extra signing implementation to track,
//!   - no `s3a` vs `s3` scheme drama (we use `s3://` everywhere, the
//!     SDK doesn't care about Hadoop scheme conventions).
//!
//! Path format: every Iceberg call to this Storage uses an
//! `s3://<bucket>/<key>` URL. Memory-only writers (FileWrite) buffer
//! in-memory and flush via `PutObject` on `close`. This is fine
//! because:
//!   - Iceberg metadata files are small (KB),
//!   - sample data files written by the flusher are bounded (the
//!     flush trigger is 64 MiB on our side, and S3 single-PUT supports
//!     up to 5 GiB).
//! When that ceases to be true we'll swap the writer for multipart
//! upload here.

use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use iceberg::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OutputFile, Storage, StorageConfig,
    StorageFactory,
};
use iceberg::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

const S3_PREFIX: &str = "s3://";

fn parse_s3_path(path: &str) -> Result<(&str, &str)> {
    let stripped = path.strip_prefix(S3_PREFIX).ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("expected an s3:// URL, got {path}"),
        )
    })?;
    stripped.split_once('/').ok_or_else(|| {
        Error::new(
            ErrorKind::DataInvalid,
            format!("s3 URL is missing a key: {path}"),
        )
    })
}

fn unexpected(context: impl Into<String>, err: impl std::fmt::Display) -> Error {
    Error::new(ErrorKind::Unexpected, format!("{}: {err}", context.into()))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsSdkS3Storage {
    /// SDK client. Skipped from serde because aws-sdk-s3 types are not
    /// (de)serializable. The factory always sets this on freshly-built
    /// instances; we never round-trip a Storage through serde in
    /// practice. The typetag attribute is still required for trait
    /// registration even though we don't exercise that path.
    #[serde(skip)]
    client: Option<aws_sdk_s3::Client>,
}

impl AwsSdkS3Storage {
    fn client(&self) -> Result<&aws_sdk_s3::Client> {
        self.client.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "AwsSdkS3Storage was deserialized without an SDK client; \
                 instantiate it through AwsSdkS3StorageFactory",
            )
        })
    }
}

#[async_trait]
#[typetag::serde]
impl Storage for AwsSdkS3Storage {
    async fn exists(&self, path: &str) -> Result<bool> {
        let (bucket, key) = parse_s3_path(path)?;
        let client = self.client()?;
        match client.head_object().bucket(bucket).key(key).send().await {
            Ok(_) => Ok(true),
            Err(e) => {
                let svc_err = e.into_service_error();
                if matches!(svc_err, HeadObjectError::NotFound(_)) {
                    Ok(false)
                } else {
                    Err(unexpected(format!("head_object {path}"), svc_err))
                }
            }
        }
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        let (bucket, key) = parse_s3_path(path)?;
        let client = self.client()?;
        let resp = client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| unexpected(format!("head_object {path}"), e))?;
        let size = resp.content_length().unwrap_or(0).max(0) as u64;
        Ok(FileMetadata { size })
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        let (bucket, key) = parse_s3_path(path)?;
        let client = self.client()?;
        let resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| unexpected(format!("get_object {path}"), e))?;
        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| unexpected(format!("read body {path}"), e))?;
        Ok(data.into_bytes())
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        let (bucket, key) = parse_s3_path(path)?;
        Ok(Box::new(AwsSdkS3FileRead {
            client: self.client()?.clone(),
            bucket: bucket.to_string(),
            key: key.to_string(),
        }))
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        let (bucket, key) = parse_s3_path(path)?;
        let client = self.client()?;
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(bs))
            .send()
            .await
            .map_err(|e| unexpected(format!("put_object {path}"), e))?;
        Ok(())
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        let (bucket, key) = parse_s3_path(path)?;
        Ok(Box::new(AwsSdkS3FileWrite {
            client: self.client()?.clone(),
            bucket: bucket.to_string(),
            key: key.to_string(),
            buffer: Mutex::new(Some(Vec::new())),
        }))
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let (bucket, key) = parse_s3_path(path)?;
        let client = self.client()?;
        client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| unexpected(format!("delete_object {path}"), e))?;
        Ok(())
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        let (bucket, prefix) = parse_s3_path(path)?;
        let client = self.client()?;
        let mut continuation: Option<String> = None;
        loop {
            let mut req = client.list_objects_v2().bucket(bucket).prefix(prefix);
            if let Some(token) = &continuation {
                req = req.continuation_token(token.clone());
            }
            let resp = req
                .send()
                .await
                .map_err(|e| unexpected(format!("list_objects_v2 {path}"), e))?;
            for obj in resp.contents() {
                if let Some(k) = obj.key() {
                    client
                        .delete_object()
                        .bucket(bucket)
                        .key(k)
                        .send()
                        .await
                        .map_err(|e| unexpected(format!("delete_object {k}"), e))?;
                }
            }
            if resp.is_truncated().unwrap_or(false) {
                match resp.next_continuation_token() {
                    Some(t) => continuation = Some(t.to_string()),
                    None => break,
                }
            } else {
                break;
            }
        }
        Ok(())
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }
}

#[derive(Debug)]
struct AwsSdkS3FileRead {
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
}

#[async_trait]
impl FileRead for AwsSdkS3FileRead {
    async fn read(&self, range: Range<u64>) -> Result<Bytes> {
        // S3 Range header: `bytes=start-end` (end inclusive).
        let header = format!("bytes={}-{}", range.start, range.end.saturating_sub(1));
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .range(header)
            .send()
            .await
            .map_err(|e| {
                unexpected(
                    format!("get_object range {}-{}", range.start, range.end),
                    e,
                )
            })?;
        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| unexpected("read body", e))?;
        Ok(data.into_bytes())
    }
}

#[derive(Debug)]
struct AwsSdkS3FileWrite {
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
    /// Bytes buffered in memory until `close` triggers a single
    /// `PutObject`. `None` after close to make double-close detectable.
    buffer: Mutex<Option<Vec<u8>>>,
}

#[async_trait]
impl FileWrite for AwsSdkS3FileWrite {
    async fn write(&mut self, bs: Bytes) -> Result<()> {
        let mut guard = self.buffer.lock().await;
        let buf = guard.as_mut().ok_or_else(|| {
            Error::new(ErrorKind::DataInvalid, "Cannot write to a closed file")
        })?;
        buf.extend_from_slice(&bs);
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let buf = {
            let mut guard = self.buffer.lock().await;
            guard
                .take()
                .ok_or_else(|| Error::new(ErrorKind::DataInvalid, "File already closed"))?
        };
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&self.key)
            .body(ByteStream::from(buf))
            .send()
            .await
            .map_err(|e| unexpected("put_object on close", e))?;
        Ok(())
    }
}

/// Storage factory that holds a pre-resolved AWS SDK `SdkConfig`.
///
/// Construct with [`AwsSdkS3StorageFactory::new`] from an async context
/// (`aws_config::defaults(...).load().await`); subsequent `build()`
/// calls (which the catalog invokes per-path lazily) then create
/// inexpensive `aws_sdk_s3::Client` instances reusing that config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsSdkS3StorageFactory {
    #[serde(skip)]
    sdk_config: Option<aws_config::SdkConfig>,
}

impl AwsSdkS3StorageFactory {
    pub fn new(sdk_config: aws_config::SdkConfig) -> Self {
        Self {
            sdk_config: Some(sdk_config),
        }
    }
}

#[typetag::serde]
impl StorageFactory for AwsSdkS3StorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        let sdk_config = self.sdk_config.as_ref().ok_or_else(|| {
            Error::new(
                ErrorKind::Unexpected,
                "AwsSdkS3StorageFactory was deserialized without SDK config",
            )
        })?;
        let client = aws_sdk_s3::Client::new(sdk_config);
        Ok(Arc::new(AwsSdkS3Storage {
            client: Some(client),
        }))
    }
}
