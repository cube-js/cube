//! DogStatsD client to report metrics over UDP.
//!
//! Can operate in `statsd`-compatible mode, see [init_metrics].
//!
//! Applications should call [init_metrics] once during global init. To report metrics, create one
//! with [counter], [gauge], [histogram] or [distribution] and send updates by calling corresponding
//! methods on the created objects. See DataDog documentation for more information on different
//! metric types.
//!
//! Code does not do any sampling or buffering at the time. Too frequent metric updates can cause
//! load on consuming servers or loose UDP packets entirely. We prefer to report only not very
//! frequently updated metrics for now to avoid more complex implementation. We can consider
//! improvements if this turns insufficient for our needs.
//!
//! Note that misconfiguration (invalid port, address, etc) can cause metric updates to be silently
//! ignored. This is by design to avoid interrupting normal operation.
use crate::CubeError;
use std::net::ToSocketAddrs;
use std::net::UdpSocket;
use std::sync::Once;

#[derive(Debug, PartialEq, Eq)]
pub enum Compatibility {
    DogStatsD,
    /// StatsD does not support distribution and histogram metric types.
    /// We replace them with timer in this mode, which provides similar functionality.
    StatsD,
}
/// Call once on application startup to initialize the metrics client.
/// Will if called twice or metrics were reported before calling this function.
pub fn init_metrics(
    bind_addr: impl ToSocketAddrs,
    server_addr: impl ToSocketAddrs,
    c: Compatibility,
) {
    global_sink::init(bind_addr, server_addr, c).unwrap()
}

pub const fn counter(name: &'static str) -> Counter {
    Counter {
        metric: Metric::new(name, MetricType::Counter),
    }
}

pub const fn gauge(name: &'static str) -> IntMetric {
    IntMetric {
        metric: Metric::new(name, MetricType::Gauge),
    }
}

pub const fn histogram(name: &'static str) -> IntMetric {
    IntMetric {
        metric: Metric::new(name, MetricType::Histogram),
    }
}

pub const fn distribution(name: &'static str) -> IntMetric {
    IntMetric {
        metric: Metric::new(name, MetricType::Distribution),
    }
}

pub struct Counter {
    metric: Metric,
}

impl Counter {
    pub fn add(&self, v: i64) {
        if let Some(s) = sink() {
            s.send(&self.metric, v)
        }
    }

    pub fn increment(&self) {
        self.add(1)
    }
}

pub struct IntMetric {
    metric: Metric,
}

impl IntMetric {
    pub fn report(&self, v: i64) {
        if let Some(s) = sink() {
            s.send(&self.metric, v)
        }
    }
}

pub type Gauge = IntMetric;
pub type Histogram = IntMetric;
pub type Distribution = IntMetric;

enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Distribution,
}

pub struct Metric {
    name: &'static str,
    kind: MetricType,
}

impl Metric {
    const fn new(name: &'static str, kind: MetricType) -> Metric {
        Metric { name, kind }
    }
}

struct Sink {
    socket: UdpSocket,
    mode: Compatibility,
}

impl Sink {
    fn connect(
        bind_addr: impl ToSocketAddrs,
        addr: impl ToSocketAddrs,
        mode: Compatibility,
    ) -> Result<Sink, CubeError> {
        let socket = UdpSocket::bind(bind_addr)?;
        socket.connect(addr)?;
        socket.set_nonblocking(true)?;
        Ok(Sink { socket, mode })
    }

    fn send(&self, m: &Metric, value: i64) {
        let kind = match m.kind {
            MetricType::Counter => "c",
            MetricType::Gauge => "g",
            MetricType::Histogram if self.mode == Compatibility::StatsD => "ms",
            MetricType::Histogram => "h",
            MetricType::Distribution if self.mode == Compatibility::StatsD => "ms",
            MetricType::Distribution => "d",
        };
        if let Err(e) = self
            .socket
            .send(format!("{}:{}|{}", m.name, value, kind).as_bytes())
        {
            log::error!("failed to send metrics: {}", e)
        }
    }
}

mod global_sink {
    use super::*;
    static mut GLOBAL_SINK: Option<Sink> = None;
    static ONCE: Once = Once::new();

    pub fn init(
        bind_addr: impl ToSocketAddrs,
        server_addr: impl ToSocketAddrs,
        c: Compatibility,
    ) -> Result<(), CubeError> {
        let s = Sink::connect(bind_addr, server_addr, c)?;

        let mut called = false;
        ONCE.call_once(|| {
            unsafe {
                GLOBAL_SINK = Some(s);
            }
            called = true;
        });
        if !called {
            panic!("Metrics initialized twice or used before initialization");
        }
        Ok(())
    }

    pub(super) fn sink() -> &'static Option<Sink> {
        // Ensure we synchronize access to GLOBAL_SINK.
        ONCE.call_once(|| {});
        unsafe { &GLOBAL_SINK }
    }
}

use global_sink::sink;
