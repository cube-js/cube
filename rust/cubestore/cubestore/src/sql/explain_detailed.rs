//! `EXPLAIN ANALYZE DETAILED`: drives a real query execution under per-query
//! tracing and renders the collected `QueryTrace` as a compact tree + category
//! summary. Split out of `sql/mod.rs` — the rendering is pure presentation and the
//! orchestration is self-contained.

use std::sync::Arc;

use datafusion::sql::parser::Statement as DFStatement;
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use sqlparser::ast::Statement;

use crate::metastore::{Column, ColumnType};
use crate::queryplanner::QueryPlan;
use crate::sql::InlineTables;
use crate::store::DataFrame;
use crate::table::{Row, TableValue};
use crate::trace::{OpKind, OpSample, QueryTrace, RouterTrace};
use crate::CubeError;

impl super::SqlServiceImpl {
    pub(crate) async fn explain_detailed(
        &self,
        statement: Statement,
    ) -> Result<Arc<DataFrame>, CubeError> {
        let ctx = crate::trace::TraceCtx::new();
        let main = crate::trace::scoped(Some(ctx.clone()), async move {
            let query_plan = self
                .query_planner
                .logical_plan(
                    DFStatement::Statement(Box::new(statement)),
                    &InlineTables::new(),
                    None,
                )
                .await?;
            let (serialized, workers) =
                match query_plan {
                    QueryPlan::Select(serialized, workers) => (serialized, workers),
                    QueryPlan::Meta(_) => return Err(CubeError::user(
                        "EXPLAIN ANALYZE DETAILED is not supported for selects from system tables"
                            .to_string(),
                    )),
                };
            let serialized_plan = {
                let _g = crate::trace::OpGuard::start(OpKind::Serialize, "plan.serialize");
                serialized.to_serialized_plan()?
            };
            // Run the full router plan on a real main (a random worker, like prod), so
            // the final stages execute where they actually happen.
            let main_node = if workers.is_empty() {
                self.cluster.server_name().to_string()
            } else {
                workers[thread_rng().sample(Uniform::new(0, workers.len()))].clone()
            };
            let _g =
                crate::trace::OpGuard::start_wrapper(OpKind::Transport, "route_select_detailed");
            let main_trace = self
                .cluster
                .run_router_select_detailed(&main_node, serialized_plan)
                .await?;
            Ok::<_, CubeError>(main_trace)
        })
        .await?;

        let trace = QueryTrace {
            router: RouterTrace {
                ops: ctx.take_ops(),
            },
            main: Some(main),
        };
        Ok(Arc::new(render_query_trace(&trace)))
    }
}

fn render_query_trace(trace: &QueryTrace) -> DataFrame {
    fn fmt_dur(us: u64) -> String {
        if us >= 1_000_000 {
            format!("{:.2}s", us as f64 / 1_000_000.0)
        } else if us >= 1_000 {
            format!("{:.2}ms", us as f64 / 1_000.0)
        } else {
            format!("{}us", us)
        }
    }

    fn fmt_bytes(b: u64) -> String {
        if b >= 1 << 20 {
            format!("{:.1}MB", b as f64 / (1u64 << 20) as f64)
        } else if b >= 1 << 10 {
            format!("{:.1}KB", b as f64 / (1u64 << 10) as f64)
        } else {
            format!("{}B", b)
        }
    }

    fn find_elapsed(ops: &[OpSample], label: &str) -> Option<u64> {
        ops.iter().find(|o| o.label == label).map(|o| o.elapsed_us)
    }

    fn bump(totals: &mut Vec<(String, u64)>, key: &str, v: u64) {
        match totals.iter_mut().find(|(k, _)| k == key) {
            Some(e) => e.1 += v,
            None => totals.push((key.to_string(), v)),
        }
    }

    // Wrapper spans contain other measured ops (round-trips, the execute span
    // around node metrics, choose_index around metastore), so they are excluded
    // from the category summary to keep categories a non-overlapping partition.
    fn add_ops(totals: &mut Vec<(String, u64)>, ops: &[OpSample]) {
        for op in ops {
            if !op.is_wrapper {
                bump(totals, &format!("{:?}", op.kind), op.elapsed_us);
            }
        }
    }

    // Sum elapsed by category over the given op slices, plus derived transport.
    fn cat_totals(op_slices: &[&[OpSample]], transport_us: u64) -> Vec<(String, u64)> {
        let mut t = Vec::new();
        for ops in op_slices {
            add_ops(&mut t, ops);
        }
        if transport_us > 0 {
            bump(&mut t, "Transport", transport_us);
        }
        t.sort_by(|a, b| b.1.cmp(&a.1));
        t
    }

    fn emit_cats(out: &mut String, title: &str, cats: &[(String, u64)]) {
        out.push_str(&format!("{}\n", title));
        let w = cats.iter().map(|(k, _)| k.len()).max().unwrap_or(0);
        for (k, v) in cats {
            out.push_str(&format!("  {:<w$}  {:>9}\n", k, fmt_dur(*v), w = w));
        }
    }

    // Renders one region (a node in the topology) as an indented block: header,
    // then `kind  label  value` lines aligned within the region, then the plan.
    fn region(
        out: &mut String,
        depth: usize,
        header: &str,
        total: Option<u64>,
        ops: &[OpSample],
        transports: &[(&str, u64)],
        memory: Option<u64>,
        plan: Option<&str>,
    ) {
        let pad = "  ".repeat(depth);
        match total {
            Some(t) => out.push_str(&format!("{}{}  ·  total {}\n", pad, header, fmt_dur(t))),
            None => out.push_str(&format!("{}{}\n", pad, header)),
        }
        let ipad = format!("{}  ", pad);

        let mut entries: Vec<(String, String, String)> = Vec::new();
        for op in ops {
            let mut v = format!("{:>9}", fmt_dur(op.elapsed_us));
            if let Some(b) = op.bytes {
                v.push_str(&format!("  {:>8}", fmt_bytes(b)));
            }
            if let Some(r) = op.rows {
                v.push_str(&format!("  {:>9} rows", r));
            }
            entries.push((format!("{:?}", op.kind), op.label.clone(), v));
        }
        for (label, us) in transports {
            entries.push((
                "Transport".to_string(),
                label.to_string(),
                format!("{:>9}", fmt_dur(*us)),
            ));
        }
        if let Some(m) = memory {
            entries.push((
                "Memory".to_string(),
                "exec.peak".to_string(),
                format!("{:>9}", fmt_bytes(m)),
            ));
        }

        let kw = entries.iter().map(|(k, _, _)| k.len()).max().unwrap_or(0);
        let lw = entries.iter().map(|(_, l, _)| l.len()).max().unwrap_or(0);
        for (kind, label, value) in &entries {
            out.push_str(&format!(
                "{}{:<kw$}  {:<lw$}  {}\n",
                ipad,
                kind,
                label,
                value,
                kw = kw,
                lw = lw
            ));
        }
        if let Some(p) = plan {
            out.push_str(&format!("{}plan:\n", ipad));
            for line in p.lines() {
                out.push_str(&format!("{}    {}\n", ipad, line));
            }
        }
    }

    let mut out = String::new();

    // ---- tree (per-node total in each header) ----
    region(
        &mut out,
        0,
        "router",
        None,
        &trace.router.ops,
        &[],
        None,
        None,
    );
    if let Some(main) = &trace.main {
        let et = find_elapsed(&trace.router.ops, "route_select_detailed")
            .map(|rt| rt.saturating_sub(main.total_us));
        let t: Vec<(&str, u64)> = et
            .map(|v| vec![("transport.entry_to_main", v)])
            .unwrap_or_default();
        region(
            &mut out,
            1,
            &format!("main · {}", main.node_name),
            Some(main.total_us),
            &main.ops,
            &t,
            main.exec_memory_peak_bytes,
            main.physical_plan.as_deref(),
        );
        for w in &main.workers {
            let mut wt: Vec<(&str, u64)> = Vec::new();
            if let Some(rt) = w.net_roundtrip_us {
                wt.push(("transport.main_to_worker", rt.saturating_sub(w.total_us)));
            }
            region(
                &mut out,
                2,
                &format!("worker · {}", w.node_name),
                Some(w.total_us),
                &w.ops,
                &wt,
                None,
                None,
            );
            if let Some(sub) = &w.subprocess {
                let mut st: Vec<(&str, u64)> = Vec::new();
                if let Some(rt) = find_elapsed(&w.ops, "ipc.select") {
                    st.push(("transport.ipc", rt.saturating_sub(sub.total_us)));
                }
                region(
                    &mut out,
                    3,
                    &format!("subprocess · {}", w.node_name),
                    Some(sub.total_us),
                    &sub.ops,
                    &st,
                    sub.exec_memory_peak_bytes,
                    sub.physical_plan.as_deref(),
                );
            }
        }
    }

    // ---- summary by category (overall + per node) ----
    out.push_str("\n────────────────────────────\n");
    out.push_str("summary by category  (transport = wire+queue; wrapper spans excluded)\n\n");
    if let Some(main) = &trace.main {
        let et = find_elapsed(&trace.router.ops, "route_select_detailed")
            .map(|rt| rt.saturating_sub(main.total_us))
            .unwrap_or(0);
        let mut overall_slices: Vec<&[OpSample]> =
            vec![trace.router.ops.as_slice(), main.ops.as_slice()];
        let mut overall_transport = et;
        for w in &main.workers {
            overall_slices.push(w.ops.as_slice());
            if let Some(rt) = w.net_roundtrip_us {
                overall_transport += rt.saturating_sub(w.total_us);
            }
            if let Some(sub) = &w.subprocess {
                overall_slices.push(sub.ops.as_slice());
                if let Some(rt) = find_elapsed(&w.ops, "ipc.select") {
                    overall_transport += rt.saturating_sub(sub.total_us);
                }
            }
        }
        emit_cats(
            &mut out,
            "overall",
            &cat_totals(&overall_slices, overall_transport),
        );
        out.push('\n');
        emit_cats(
            &mut out,
            "router",
            &cat_totals(&[trace.router.ops.as_slice()], 0),
        );
        out.push('\n');
        emit_cats(
            &mut out,
            &format!("main · {}", main.node_name),
            &cat_totals(&[main.ops.as_slice()], et),
        );
        for w in &main.workers {
            out.push('\n');
            let mut slices: Vec<&[OpSample]> = vec![w.ops.as_slice()];
            let mut wtrans = w
                .net_roundtrip_us
                .map(|rt| rt.saturating_sub(w.total_us))
                .unwrap_or(0);
            if let Some(sub) = &w.subprocess {
                slices.push(sub.ops.as_slice());
                if let Some(rt) = find_elapsed(&w.ops, "ipc.select") {
                    wtrans += rt.saturating_sub(sub.total_us);
                }
            }
            emit_cats(
                &mut out,
                &format!("worker · {}", w.node_name),
                &cat_totals(&slices, wtrans),
            );
        }
    } else {
        emit_cats(
            &mut out,
            "overall",
            &cat_totals(&[trace.router.ops.as_slice()], 0),
        );
    }

    DataFrame::new(
        vec![Column::new("trace".to_string(), ColumnType::String, 0)],
        vec![Row::new(vec![TableValue::String(out)])],
    )
}
