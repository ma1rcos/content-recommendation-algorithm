//! Circuit breaker mínimo para o motor Python.
//!
//! Justificativa: num cache miss, a API chama o motor de forma síncrona. Se o
//! motor estiver fora ou lento, cada requisição esperaria o timeout inteiro —
//! e o acúmulo de requisições presas viraria uma cascata de falhas. Com o
//! breaker aberto, a API vai direto ao fallback (trending) por `open_for`, e
//! depois deixa passar uma tentativa (meio-aberto).

use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::{Duration, Instant};

pub struct Breaker {
    threshold: u32,
    open_for: Duration,
    failures: AtomicU32,
    /// Instante (ms desde `epoch`) até quando o circuito fica aberto; 0 = fechado.
    open_until_ms: AtomicI64,
    epoch: Instant,
}

impl Breaker {
    pub fn new(threshold: u32, open_for: Duration) -> Self {
        Self {
            threshold,
            open_for,
            failures: AtomicU32::new(0),
            open_until_ms: AtomicI64::new(0),
            epoch: Instant::now(),
        }
    }

    fn now_ms(&self) -> i64 {
        self.epoch.elapsed().as_millis() as i64
    }

    /// `true` se a chamada pode prosseguir. Ao expirar o período aberto, uma
    /// única chamada passa (compare_exchange) para testar o motor.
    pub fn allow(&self) -> bool {
        let until = self.open_until_ms.load(Ordering::Acquire);
        if until == 0 {
            return true;
        }
        let now = self.now_ms();
        if now < until {
            return false;
        }
        // meio-aberto: reabre por mais um período e deixa ESTA chamada testar.
        self.open_until_ms
            .compare_exchange(
                until,
                now + self.open_for.as_millis() as i64,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub fn record_success(&self) {
        self.failures.store(0, Ordering::Release);
        self.open_until_ms.store(0, Ordering::Release);
        crate::metrics::BREAKER_OPEN.set(0);
    }

    pub fn record_failure(&self) {
        let n = self.failures.fetch_add(1, Ordering::AcqRel) + 1;
        if n >= self.threshold {
            self.open_until_ms.store(
                self.now_ms() + self.open_for.as_millis() as i64,
                Ordering::Release,
            );
            crate::metrics::BREAKER_OPEN.set(1);
        }
    }

    pub fn is_open(&self) -> bool {
        let until = self.open_until_ms.load(Ordering::Acquire);
        until != 0 && self.now_ms() < until
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opens_after_threshold_and_half_opens_after_period() {
        let b = Breaker::new(2, Duration::from_millis(30));
        assert!(b.allow());
        b.record_failure();
        assert!(b.allow());
        b.record_failure();
        assert!(!b.allow() && b.is_open());
        std::thread::sleep(Duration::from_millis(40));
        assert!(b.allow(), "uma chamada de teste passa");
        assert!(!b.allow(), "as demais continuam bloqueadas");
        b.record_success();
        assert!(b.allow() && !b.is_open());
    }
}
