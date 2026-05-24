use crate::{RagloomError, RagloomErrorKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ObjectMeta {
    pub key: String,
    pub size_bytes: u64,
    pub mtime_unix_secs: i64,
    pub etag: Option<String>,
}

pub trait S3Client: Send + Sync + std::fmt::Debug {
    fn bucket_name(&self) -> &str;
    fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<S3ObjectMeta>, RagloomError>;
    fn get_object(&self, key: &str) -> Result<Vec<u8>, RagloomError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3Location<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
}

pub fn parse_s3_uri(uri: &str) -> Result<S3Location<'_>, RagloomError> {
    let Some(rest) = uri.strip_prefix("s3://") else {
        return Err(RagloomError::from_kind(RagloomErrorKind::InvalidInput)
            .with_context("expected s3://bucket/key canonical path"));
    };
    let Some((bucket, key)) = rest.split_once('/') else {
        return Err(RagloomError::from_kind(RagloomErrorKind::InvalidInput)
            .with_context("expected s3://bucket/key canonical path"));
    };
    if bucket.trim().is_empty() || key.is_empty() {
        return Err(RagloomError::from_kind(RagloomErrorKind::InvalidInput)
            .with_context("expected s3://bucket/key canonical path"));
    }
    Ok(S3Location { bucket, key })
}

pub fn canonical_s3_path(bucket: &str, key: &str) -> String {
    format!("s3://{bucket}/{key}")
}

#[derive(Debug, Clone)]
pub struct RustS3Client {
    bucket_name: String,
    bucket: Box<s3::Bucket>,
}

impl RustS3Client {
    pub fn from_default_env(bucket_name: &str) -> Result<Self, RagloomError> {
        let region = resolve_region_from_env()?;
        let credentials = s3::creds::Credentials::default().map_err(|e| {
            RagloomError::new(RagloomErrorKind::Config, e)
                .with_context("failed to load S3 credentials from the default environment chain")
        })?;
        let bucket = s3::Bucket::new(bucket_name, region, credentials).map_err(|e| {
            RagloomError::new(RagloomErrorKind::Config, e).with_context(format!(
                "failed to configure S3 bucket client for {bucket_name}"
            ))
        })?;
        Ok(Self {
            bucket_name: bucket_name.to_string(),
            bucket,
        })
    }
}

impl S3Client for RustS3Client {
    fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    fn list_objects(&self, prefix: Option<&str>) -> Result<Vec<S3ObjectMeta>, RagloomError> {
        let pages = self
            .bucket
            .list(prefix.unwrap_or_default().to_string(), None)
            .map_err(|e| {
                RagloomError::new(RagloomErrorKind::Io, e).with_context(format!(
                    "failed to list S3 objects in bucket {}",
                    self.bucket_name
                ))
            })?;
        let mut objects = Vec::new();

        for page in pages {
            for object in page.contents {
                objects.push(S3ObjectMeta {
                    key: object.key,
                    size_bytes: object.size,
                    mtime_unix_secs: parse_last_modified(&object.last_modified)?,
                    etag: object.e_tag,
                });
            }
        }

        Ok(objects)
    }

    fn get_object(&self, key: &str) -> Result<Vec<u8>, RagloomError> {
        let response = self.bucket.get_object(key).map_err(|e| {
            RagloomError::new(RagloomErrorKind::Io, e).with_context(format!(
                "failed to get S3 object s3://{}/{key}",
                self.bucket_name
            ))
        })?;
        let status = response.status_code();
        if !(200..300).contains(&status) {
            return Err(
                RagloomError::from_kind(RagloomErrorKind::Io).with_context(format!(
                    "failed to get S3 object s3://{}/{key}: status {status}",
                    self.bucket_name
                )),
            );
        }
        Ok(response.into_bytes().to_vec())
    }
}

fn resolve_region_from_env() -> Result<s3::Region, RagloomError> {
    let region = std::env::var("AWS_REGION")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            std::env::var("AWS_DEFAULT_REGION")
                .ok()
                .filter(|value| !value.trim().is_empty())
        })
        .ok_or_else(|| {
            RagloomError::from_kind(RagloomErrorKind::Config)
                .with_context("missing AWS region; set AWS_REGION or AWS_DEFAULT_REGION")
        })?;
    region.parse().map_err(|e| {
        RagloomError::new(RagloomErrorKind::Config, e)
            .with_context(format!("invalid AWS region {region}"))
    })
}

fn parse_last_modified(value: &str) -> Result<i64, RagloomError> {
    time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map(|timestamp| timestamp.unix_timestamp())
        .map_err(|e| {
            RagloomError::new(RagloomErrorKind::InvalidInput, e)
                .with_context(format!("invalid S3 last_modified timestamp {value}"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_s3_uri_preserves_exact_key() {
        let location = parse_s3_uri("s3://docs-bucket/kb//a.md").expect("parse s3 uri");
        assert_eq!(location.bucket, "docs-bucket");
        assert_eq!(location.key, "kb//a.md");
    }

    #[test]
    fn parse_last_modified_accepts_rfc3339() {
        let timestamp = parse_last_modified("2026-05-23T12:34:56.000Z").expect("parse timestamp");
        assert_eq!(timestamp, 1_779_539_696);
    }
}
