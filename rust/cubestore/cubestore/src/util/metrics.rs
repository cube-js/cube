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
    mode: Compatibility,
    constant_tags: Vec<String>,
) {
    global_sink::init(bind_addr, server_addr, mode, constant_tags).unwrap()
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

pub fn format_tag(name: &'static str, value: &str) -> String {
    format!("{}:{}", name, value)
}

pub struct Counter {
    metric: Metric,
}

impl Counter {
    pub fn add(&self, v: i64) {
        self.add_with_tags(v, None)
    }

    pub fn add_with_tags(&self, v: i64, tags: Option<&Vec<String>>) {
        if let Some(s) = sink() {
            s.send(&self.metric, v, tags)
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
        self.report_with_tags(v, None)
    }

    pub fn report_with_tags(&self, v: i64, tags: Option<&Vec<String>>) {
        if let Some(s) = sink() {
            s.send(&self.metric, v, tags)
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
    constant_tags: Option<String>,
}

impl Sink {
    fn connect(
        bind_addr: impl ToSocketAddrs,
        addr: impl ToSocketAddrs,
        mode: Compatibility,
        tags: Vec<String>,
    ) -> Result<Sink, CubeError> {
        let socket = UdpSocket::bind(bind_addr)?;
        socket.connect(addr)?;
        socket.set_nonblocking(true)?;

        let constant_tags = if tags.len() > 0 {
            Some(tags.join(","))
        } else {
            None
        };

        Ok(Sink {
            socket,
            mode,
            constant_tags,
        })
    }

    fn send(&self, m: &Metric, value: i64, tags: Option<&Vec<String>>) {
        let kind = match m.kind {
            MetricType::Counter => "c",
            MetricType::Gauge => "g",
            MetricType::Histogram if self.mode == Compatibility::StatsD => "ms",
            MetricType::Histogram => "h",
            MetricType::Distribution if self.mode == Compatibility::StatsD => "ms",
            MetricType::Distribution => "d",
        };
        let data = format!("{}:{}|{}", m.name, value, kind);

        let msg = match (&self.constant_tags, tags) {
            (Some(constant_tags), tags) => {
                if let Some(t) = tags {
                    format!("{}|#{},{}", data, constant_tags, &t.join(","))
                } else {
                    format!("{}|#{}", data, constant_tags)
                }
            }
            (None, tags) => {
                if let Some(t) = tags {
                    format!("{}|#{}", data, t.join(","))
                } else {
                    data
                }
            }
        };

        // We deliberately choose to loose metric submissions on failures.
        // TODO: handle EWOULDBLOCK with background sends or at least internal failure counters.
        let _ = self.socket.send(msg.as_bytes());
    }
}

mod global_sink {
    use super::*;
    use std::sync::OnceLock;

    static GLOBAL_SINK: OnceLock<Option<Sink>> = OnceLock::new();

    pub fn init(
        bind_addr: impl ToSocketAddrs,
        server_addr: impl ToSocketAddrs,
        mode: Compatibility,
        constant_tags: Vec<String>,
    ) -> Result<(), CubeError> {
        let s = Sink::connect(bind_addr, server_addr, mode, constant_tags)?;

        let mut called = false;
        GLOBAL_SINK.get_or_init(|| {
            called = true;
            Some(s)
        });
        if !called {
            panic!("Metrics initialized twice or used before initialization");
        }
        Ok(())
    }

    pub(super) fn sink() -> &'static Option<Sink> {
        GLOBAL_SINK.get_or_init(|| None)
    }
}

use global_sink::sink;
