use crate::config::ConfigObj;
use crate::store::compaction::CompactionService;
use crate::util::WorkerLoop;
use crate::CubeError;
use datafusion::cube_ext;
use futures_timer::Delay;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
pub struct MemoryCompactionRunner {
    config: Arc<dyn ConfigObj>,
    compaction_service: Arc<dyn CompactionService>,
    server_name: String,
    stop_token: CancellationToken,
    run_loop: WorkerLoop,
}

impl MemoryCompactionRunner {
    pub fn new(
        config: Arc<dyn ConfigObj>,
        compaction_service: Arc<dyn CompactionService>,
        server_name: String,
        stop_token: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            config,
            compaction_service,
            server_name,
            stop_token,
            run_loop: WorkerLoop::new("InMemoryCompaction"),
        })
    }

    pub fn spawn_processing_loop(self_ref: Arc<Self>) -> JoinHandle<()> {
        let self_to_move = self_ref.clone();
        cube_ext::spawn(async move {
            self_to_move
                .run_loop
                .process(
                    self_to_move.clone(),
                    async move |_| Ok(Delay::new(Duration::from_secs(5)).await),
                    async move |s, _| s.compact_in_memory_chunks().await,
                )
                .await;
        })
    }

    async fn compact_in_memory_chunks(&self) -> Result<(), CubeError> {
        let node_name = self.server_name.clone();
        self.compaction_service
            .compact_node_in_memory_chunks(node_name)
            .await
    }
}
