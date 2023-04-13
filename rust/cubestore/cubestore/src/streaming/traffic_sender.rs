use crate::telemetry::incoming_traffic_agent_event;
use crate::CubeError;
use std::sync::Arc;
use std::time::SystemTime;

pub struct TrafficSender {
    trace_obj: Option<String>,
    traffic_bytes: u64,
    last_sended: SystemTime,
}

impl TrafficSender {
    pub fn new(trace_obj: Option<String>) -> Arc<Self> {
        Arc::new(Self {
            trace_obj,
            traffic_bytes: 0,
            last_sended: SystemTime::now(),
        })
    }

    pub fn process_event(&mut self, bytes: u64) -> Result<(), CubeError> {
        if self.trace_obj.is_none() {
            return Ok(());
        }
        self.traffic_bytes += bytes;
        if self.traffic_bytes > 0 && self.last_sended.elapsed()?.as_secs() > 5 * 60 {
            incoming_traffic_agent_event(self.trace_obj.as_ref().unwrap(), self.traffic_bytes)?;
            self.traffic_bytes = 0;
            self.last_sended = SystemTime::now();
        }
        Ok(())
    }
}

impl Drop for TrafficSender {
    fn drop(&mut self) {
        if self.trace_obj.is_some() && self.traffic_bytes > 0 {
            incoming_traffic_agent_event(self.trace_obj.as_ref().unwrap(), self.traffic_bytes);
        }
    }
}
