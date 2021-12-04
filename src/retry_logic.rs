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
        // TODO: Non-retryable errs

        if attempt_number >= self.maximum_attempts.max(0) as usize {
            return None;
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
            Some((interval.mul_f64(coeff.powi(attempt_number as i32))).min(max_iv))
        } else {
            // No retries if initial interval is not specified
            None
        }
    }
}
