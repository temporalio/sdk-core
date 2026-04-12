//! Shared error types used across Temporal SDK crates.

use crate::protos::temporal::api::{
    common::v1::Payloads,
    enums::v1::ApplicationErrorCategory,
    failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
};
use std::time::Duration;

/// User-authored application failure metadata that can be converted into a Temporal failure.
#[derive(Debug, bon::Builder)]
#[builder(start_fn = builder, state_mod(vis = "pub"))]
pub struct ApplicationFailure {
    #[builder(start_fn, into)]
    source: anyhow::Error,
    type_name: Option<String>,
    #[builder(default)]
    non_retryable: bool,
    next_retry_delay: Option<Duration>,
    #[builder(default = ApplicationErrorCategory::Unspecified)]
    category: ApplicationErrorCategory,
    details: Option<Payloads>,
}

impl ApplicationFailure {
    /// Construct a retryable application failure with no extra metadata.
    pub fn new(source: impl Into<anyhow::Error>) -> Self {
        Self {
            source: source.into(),
            type_name: None,
            non_retryable: false,
            next_retry_delay: None,
            category: ApplicationErrorCategory::Unspecified,
            details: None,
        }
    }

    /// Construct a non-retryable application failure with no extra metadata.
    pub fn non_retryable(source: impl Into<anyhow::Error>) -> Self {
        Self {
            non_retryable: true,
            ..Self::new(source)
        }
    }

    /// Returns the wrapped source error.
    pub fn source_error(&self) -> &anyhow::Error {
        &self.source
    }

    /// Returns the configured application failure type name, if any.
    pub fn type_name(&self) -> Option<&str> {
        self.type_name.as_deref()
    }

    /// Returns true if this failure should be treated as non-retryable.
    pub fn is_non_retryable(&self) -> bool {
        self.non_retryable
    }

    /// Returns the explicitly configured next retry delay, if any.
    pub fn next_retry_delay(&self) -> Option<Duration> {
        self.next_retry_delay
    }

    /// Returns the application error category.
    pub fn category(&self) -> ApplicationErrorCategory {
        self.category
    }

    /// Returns the raw encoded details payloads, if any.
    pub fn details(&self) -> Option<&Payloads> {
        self.details.as_ref()
    }
}

impl std::fmt::Display for ApplicationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.source)
    }
}

impl std::error::Error for ApplicationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

impl From<anyhow::Error> for ApplicationFailure {
    fn from(value: anyhow::Error) -> Self {
        Self::new(value)
    }
}

impl From<ApplicationFailure> for Failure {
    fn from(value: ApplicationFailure) -> Self {
        let mut failure = value
            .source
            .chain()
            .rfold(None, |cause, err| {
                Some(Failure {
                    message: err.to_string(),
                    cause: cause.map(Box::new),
                    ..Default::default()
                })
            })
            .unwrap_or_default();
        failure.failure_info = Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: value.type_name.unwrap_or_default(),
                non_retryable: value.non_retryable,
                details: value.details,
                next_retry_delay: value.next_retry_delay.and_then(|d| d.try_into().ok()),
                category: value.category as i32,
            },
        ));
        failure
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protos::temporal::api::common::v1::Payload;

    #[test]
    fn constructors_set_retryability_defaults() {
        assert!(!ApplicationFailure::new(anyhow::anyhow!("retryable")).is_non_retryable());
        assert!(
            ApplicationFailure::non_retryable(anyhow::anyhow!("non-retryable")).is_non_retryable()
        );
    }

    #[test]
    fn conversion_preserves_application_metadata() {
        let payloads = Payloads {
            payloads: vec![Payload {
                data: b"details".to_vec(),
                ..Default::default()
            }],
        };
        let failure: Failure = ApplicationFailure::builder(anyhow::anyhow!("oops"))
            .type_name("MyType".to_owned())
            .non_retryable(true)
            .next_retry_delay(Duration::from_secs(3))
            .category(ApplicationErrorCategory::Benign)
            .details(payloads.clone())
            .build()
            .into();
        let Some(FailureInfo::ApplicationFailureInfo(info)) = failure.failure_info else {
            panic!("expected application failure info");
        };
        assert_eq!(failure.message, "oops");
        assert_eq!(info.r#type, "MyType");
        assert!(info.non_retryable);
        assert_eq!(info.details, Some(payloads));
        assert_eq!(info.category(), ApplicationErrorCategory::Benign);
        assert_eq!(info.next_retry_delay.unwrap().seconds, 3);
    }
}
