// Copyright 2020 Ant Group. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// A QPS (Queries Per Second) rate limiter using token bucket algorithm.
///
/// The token bucket algorithm works by:
/// - Maintaining a bucket with a maximum capacity
/// - Tokens are added at a fixed rate (determined by QPS)
/// - Each request consumes one token
/// - If no tokens are available, the request must wait
#[derive(Clone)]
pub struct QpsLimiter {
    inner: Arc<Mutex<QpsLimiterInner>>,
    condvar: Arc<Condvar>,
}

struct QpsLimiterInner {
    /// Maximum tokens in the bucket
    capacity: f64,
    /// Current tokens available
    tokens: f64,
    /// Tokens per second
    rate: f64,
    /// Last time tokens were refilled
    last_refill_time: Instant,
}

impl QpsLimiter {
    /// Create a new QPS limiter with the given QPS limit.
    ///
    /// # Arguments
    ///
    /// * `qps` - Queries per second limit (must be > 0)
    ///
    /// # Panics
    ///
    /// Panics if qps <= 0.0
    pub fn new(qps: f64) -> Self {
        assert!(qps > 0.0, "QPS must be greater than 0");

        let inner = QpsLimiterInner {
            capacity: qps,
            tokens: qps,
            rate: qps,
            last_refill_time: Instant::now(),
        };

        QpsLimiter {
            inner: Arc::new(Mutex::new(inner)),
            condvar: Arc::new(Condvar::new()),
        }
    }

    /// Try to acquire a token without blocking.
    ///
    /// Returns true if a token was acquired, false otherwise.
    pub fn try_acquire(&self) -> bool {
        self.try_acquire_with_clock(Instant::now)
    }

    fn try_acquire_with_clock(&self, clock: impl FnOnce() -> Instant) -> bool {
        let mut inner = self.inner.lock().unwrap();
        inner.refill_at(clock());

        if inner.tokens >= 1.0 {
            inner.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Acquire a token, blocking if necessary until one is available.
    ///
    /// This function uses a condition variable to efficiently wait
    /// for tokens to become available, without busy-waiting.
    ///
    /// Returns true if the request was rate-limited (had to wait), false if acquired immediately.
    pub fn acquire(&self) -> bool {
        self.acquire_with_clock(Instant::now)
    }

    fn acquire_with_clock(&self, mut clock: impl FnMut() -> Instant) -> bool {
        if self.try_acquire_with_clock(&mut clock) {
            return false;
        }

        let mut inner = self.inner.lock().unwrap();

        loop {
            inner.refill_at(clock());

            if inner.tokens >= 1.0 {
                inner.tokens -= 1.0;
                return true;
            }

            // Calculate timeout for the next refill
            let shortage = 1.0 - inner.tokens;
            let timeout = Duration::from_secs_f64(shortage / inner.rate);

            inner = self.condvar.wait_timeout(inner, timeout).unwrap().0;
        }
    }

    /// Try to acquire multiple tokens without blocking.
    ///
    /// Returns true if all tokens were acquired, false otherwise.
    pub fn try_acquire_tokens(&self, count: f64) -> bool {
        self.try_acquire_tokens_with_clock(count, Instant::now)
    }

    fn try_acquire_tokens_with_clock(&self, count: f64, clock: impl FnOnce() -> Instant) -> bool {
        assert!(count > 0.0, "Token count must be greater than 0");

        let mut inner = self.inner.lock().unwrap();
        inner.refill_at(clock());

        if inner.tokens >= count {
            inner.tokens -= count;
            true
        } else {
            false
        }
    }

    /// Acquire multiple tokens, blocking if necessary.
    ///
    /// Uses a condition variable to efficiently wait for tokens.
    ///
    /// Returns true if the request was rate-limited (had to wait), false if acquired immediately.
    pub fn acquire_tokens(&self, count: f64) -> bool {
        self.acquire_tokens_with_clock(count, Instant::now)
    }

    fn acquire_tokens_with_clock(&self, count: f64, mut clock: impl FnMut() -> Instant) -> bool {
        assert!(count > 0.0, "Token count must be greater than 0");

        if self.try_acquire_tokens_with_clock(count, &mut clock) {
            return false;
        }

        let mut inner = self.inner.lock().unwrap();

        loop {
            inner.refill_at(clock());

            if inner.tokens >= count {
                inner.tokens -= count;
                return true;
            }

            // Calculate timeout for the next refill
            let shortage = count - inner.tokens;
            let timeout = Duration::from_secs_f64(shortage / inner.rate);

            inner = self.condvar.wait_timeout(inner, timeout).unwrap().0;
        }
    }

    /// Get the current number of available tokens.
    pub fn current_tokens(&self) -> f64 {
        let mut inner = self.inner.lock().unwrap();
        inner.refill();
        inner.tokens
    }

    /// Get the configured QPS rate.
    pub fn qps(&self) -> f64 {
        let inner = self.inner.lock().unwrap();
        inner.rate
    }
}

impl QpsLimiterInner {
    fn refill(&mut self) {
        self.refill_at(Instant::now());
    }

    fn refill_at(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.last_refill_time).as_secs_f64();

        // Calculate new tokens based on elapsed time
        let new_tokens = elapsed * self.rate;
        self.tokens = (self.tokens + new_tokens).min(self.capacity);

        self.last_refill_time = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn running_clock_from(now: Instant) -> impl FnMut() -> Instant {
        let mut origin: Option<Instant> = None;
        move || match origin {
            Some(origin) => now + origin.elapsed(),
            None => {
                // Start real elapsed time inside the first acquisition's lock.
                origin = Some(Instant::now());
                now
            }
        }
    }

    #[test]
    fn test_qps_limiter_creation() {
        let limiter = QpsLimiter::new(10.0);
        assert_eq!(limiter.qps(), 10.0);
    }

    #[test]
    fn test_initial_tokens() {
        let limiter = QpsLimiter::new(5.0);
        // Should have capacity tokens initially
        let tokens = limiter.current_tokens();
        assert!((tokens - 5.0).abs() < 0.01);
    }

    #[test]
    fn test_single_token_acquisition() {
        let limiter = QpsLimiter::new(10.0);

        // Should be able to acquire initial tokens
        assert!(limiter.try_acquire());
        assert!(limiter.try_acquire());
    }

    #[test]
    fn test_token_exhaustion() {
        let limiter = QpsLimiter::new(3.0);

        // Acquire all initial tokens
        assert!(limiter.try_acquire()); // 2.0 left
        assert!(limiter.try_acquire()); // 1.0 left
        assert!(limiter.try_acquire()); // 0.0 left

        // Next acquisition should fail
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn test_token_refill() {
        let limiter = QpsLimiter::new(100.0);
        let now = limiter.inner.lock().unwrap().last_refill_time;

        // Exhaust the initial bucket without allowing elapsed wall time to refill it.
        let mut acquired = 0;
        while limiter.try_acquire_with_clock(|| now) {
            acquired += 1;
        }
        assert_eq!(acquired, 100);

        let mut inner = limiter.inner.lock().unwrap();
        assert_eq!(inner.tokens, 0.0);
        let refill_time = now + Duration::from_millis(100);
        inner.refill_at(refill_time);
        assert_eq!(inner.tokens, 10.0);

        // The same instant must not refill twice, and later refills cannot exceed capacity.
        inner.refill_at(refill_time);
        assert_eq!(inner.tokens, 10.0);
        inner.refill_at(now + Duration::from_secs(2));
        assert_eq!(inner.tokens, 100.0);
    }

    #[test]
    fn test_blocking_acquire() {
        let limiter = QpsLimiter::new(100.0);
        let now = limiter.inner.lock().unwrap().last_refill_time;

        // Exhaust all tokens
        while limiter.try_acquire_with_clock(|| now) {}

        let start = Instant::now();
        let was_limited = limiter.acquire_with_clock(running_clock_from(now));
        let elapsed = start.elapsed();

        // Should have been rate-limited
        assert!(was_limited);

        // Should have waited at least a few milliseconds
        assert!(elapsed.as_millis() >= 5);
    }

    #[test]
    fn test_multiple_tokens_acquisition() {
        let limiter = QpsLimiter::new(10.0);

        // Should be able to acquire 5 tokens
        assert!(limiter.try_acquire_tokens(5.0));

        // Should have ~5 tokens left
        let tokens = limiter.current_tokens();
        assert!((4.9..=5.1).contains(&tokens));

        // Should not be able to acquire another 6 tokens
        assert!(!limiter.try_acquire_tokens(6.0));

        // But should be able to acquire 5
        assert!(limiter.try_acquire_tokens(5.0));
    }

    #[test]
    fn test_qps_accuracy() {
        let limiter = QpsLimiter::new(10.0);

        // Exhaust all tokens
        while limiter.try_acquire() {}

        // Sleep for 1 second
        std::thread::sleep(Duration::from_secs(1));

        // Should have approximately 10 tokens (±1 for timing variance)
        let tokens = limiter.current_tokens();
        assert!((8.0..=12.0).contains(&tokens));
    }

    #[test]
    fn test_low_qps() {
        let limiter = QpsLimiter::new(2.0); // 2 QPS

        // Exhaust all tokens
        assert!(limiter.try_acquire()); // 1 left
        assert!(limiter.try_acquire()); // 0 left
        assert!(!limiter.try_acquire()); // No more

        // Wait 500ms, should have ~1 token
        std::thread::sleep(Duration::from_millis(500));
        let tokens = limiter.current_tokens();
        assert!((0.8..=1.2).contains(&tokens));
    }

    #[test]
    fn test_concurrent_acquisition() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let limiter = Arc::new(QpsLimiter::new(100.0));
        let now = limiter.inner.lock().unwrap().last_refill_time;
        let barrier = Arc::new(Barrier::new(11));

        // Compete for one fixed bucket without admitting new tokens while threads run.
        let mut handles = vec![];

        for _ in 0..10 {
            let limiter = Arc::clone(&limiter);
            let barrier = Arc::clone(&barrier);
            let handle = thread::spawn(move || {
                barrier.wait();
                let mut count = 0;
                while limiter.try_acquire_with_clock(|| now) {
                    count += 1;
                }
                count
            });
            handles.push(handle);
        }
        barrier.wait();

        let mut total_acquired = 0;
        for handle in handles {
            total_acquired += handle.join().unwrap();
        }

        // Should acquire all 100 initial tokens
        assert_eq!(total_acquired, 100);

        // Should have no tokens left
        assert!(!limiter.try_acquire_with_clock(|| now));
        assert_eq!(limiter.inner.lock().unwrap().tokens, 0.0);
    }

    #[test]
    fn test_acquire_returns_rate_limited() {
        let limiter = QpsLimiter::new(10.0);

        // First acquisition with available tokens should not be rate-limited
        let was_limited = limiter.acquire();
        assert!(!was_limited);

        // Exhaust all tokens
        let now = limiter.inner.lock().unwrap().last_refill_time;
        while limiter.try_acquire_with_clock(|| now) {}

        // Next acquisition should be rate-limited
        let start = Instant::now();
        let was_limited = limiter.acquire_with_clock(running_clock_from(now));
        assert!(was_limited);
        assert!(start.elapsed() >= Duration::from_millis(50));
    }

    #[test]
    fn test_acquire_tokens_returns_rate_limited() {
        let limiter = QpsLimiter::new(10.0);

        // First acquisition with available tokens should not be rate-limited
        let was_limited = limiter.acquire_tokens(5.0);
        assert!(!was_limited);

        // Try to acquire more than available
        let now = limiter.inner.lock().unwrap().last_refill_time;
        let start = Instant::now();
        let was_limited = limiter.acquire_tokens_with_clock(10.0, running_clock_from(now));
        assert!(was_limited);
        assert!(start.elapsed() >= Duration::from_millis(400));
    }
}
