use crate::telemetry::incoming_traffic_agent_event;
use crate::CubeError;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

struct TrafficSenderState {
    traffic_bytes: u64,
    last_sended: SystemTime,
}
pub struct TrafficSender {
    trace_obj: Option<String>,
    state: Mutex<TrafficSenderState>,
}

impl TrafficSender {
    pub fn new(trace_obj: Option<String>) -> Arc<Self> {
        Arc::new(Self {
            trace_obj,
            state: Mutex::new(TrafficSenderState {
                traffic_bytes: 0,
                last_sended: SystemTime::now(),
            }),
        })
    }

    pub fn process_event(&self, bytes: u64) -> Result<(), CubeError> {
        if self.trace_obj.is_none() {
            return Ok(());
        }
        if let Ok(mut state) = self.state.lock() {
            state.traffic_bytes += bytes;
            if state.traffic_bytes > 0 && state.last_sended.elapsed()?.as_secs() > 5 * 60 {
                incoming_traffic_agent_event(
                    self.trace_obj.as_ref().unwrap(),
                    state.traffic_bytes,
                )?;
                state.traffic_bytes = 0;
                state.last_sended = SystemTime::now();
            }
        }
        Ok(())
    }
}

impl Drop for TrafficSender {
    fn drop(&mut self) {
        if self.trace_obj.is_some() {
            if let Ok(state) = self.state.lock() {
                if state.traffic_bytes > 0 {
                    let _ = incoming_traffic_agent_event(
                        self.trace_obj.as_ref().unwrap(),
                        state.traffic_bytes,
                    );
                }
            }
        }
    }
}
