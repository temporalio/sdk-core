use std::time::Duration;
use temporal_sdk_core_protos::coresdk::common::RetryPolicy;

pub(crate) trait RetryPolicyExt {
    /// Ask this retry policy if a retry should be performed. Caller provides the current attempt
    /// number - the first attempt should start at 1.
    ///
    /// Returns `None` if it should not, otherwise a duration indicating how long to wait before
    /// performing the retry.
    fn should_retry(&self, attempt_number: usize, err_str: &str) -> Option<Duration>;
}

impl RetryPolicyExt for RetryPolicy {
    fn should_retry(&self, attempt_number: usize, err_str: &str) -> Option<Duration> {
        if attempt_number >= self.maximum_attempts.max(0) as usize {
            return None;
        }

        for pat in &self.non_retryable_error_types {
            if err_str.contains(pat.as_str()) {
                return None;
            }
        }

        let converted_interval = self
            .initial_interval
            .clone()
            .map(TryInto::try_into)
            .transpose()
            .ok()
            .flatten();
        if attempt_number == 1 {
            return converted_interval;
        }
        let coeff = if self.backoff_coefficient != 0. {
            self.backoff_coefficient
        } else {
            2.0
        };

        if let Some(interval) = converted_interval {
            let max_iv = self
                .maximum_interval
                .clone()
                .map(TryInto::try_into)
                .transpose()
                .ok()
                .flatten()
                .unwrap_or_else(|| interval * 100);
            Some((interval.mul_f64(coeff.powi(attempt_number as i32 - 1))).min(max_iv))
        } else {
            // No retries if initial interval is not specified
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn calcs_backoffs_properly() {
        let rp = RetryPolicy {
            initial_interval: Some(Duration::from_secs(1).into()),
            backoff_coefficient: 2.0,
            maximum_interval: Some(Duration::from_secs(10).into()),
            maximum_attempts: 10,
            non_retryable_error_types: vec![],
        };
        let res = rp.should_retry(1, "").unwrap();
        assert_eq!(res.as_millis(), 1_000);
        let res = rp.should_retry(2, "").unwrap();
        assert_eq!(res.as_millis(), 2_000);
        let res = rp.should_retry(3, "").unwrap();
        assert_eq!(res.as_millis(), 4_000);
        let res = rp.should_retry(4, "").unwrap();
        assert_eq!(res.as_millis(), 8_000);
        let res = rp.should_retry(5, "").unwrap();
        assert_eq!(res.as_millis(), 10_000);
        let res = rp.should_retry(6, "").unwrap();
        assert_eq!(res.as_millis(), 10_000);
        // Max attempts - no retry
        assert!(rp.should_retry(10, "").is_none());
    }

    #[test]
    fn no_interval_no_backoff() {
        let rp = RetryPolicy {
            initial_interval: None,
            backoff_coefficient: 2.0,
            maximum_interval: None,
            maximum_attempts: 10,
            non_retryable_error_types: vec![],
        };
        assert!(rp.should_retry(1, "").is_none());
    }

    #[test]
    fn no_retry_err_str_match() {
        let rp = RetryPolicy {
            initial_interval: Some(Duration::from_secs(1).into()),
            backoff_coefficient: 2.0,
            maximum_interval: Some(Duration::from_secs(10).into()),
            maximum_attempts: 10,
            non_retryable_error_types: vec!["no retry".to_string()],
        };
        assert!(rp.should_retry(1, "i have no retry match").is_none());
    }
}
