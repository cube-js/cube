use std::sync::Arc;

use log::debug;
use serde_derive::Deserialize;
use warp::{Filter, Rejection};

use crate::sql::SqlService;
use crate::CubeError;

#[derive(Deserialize, Debug)]
pub struct SqlQueryBody {
    query: String,
}

pub async fn run_server(sql_service: Arc<dyn SqlService>) -> Result<(), CubeError> {
    let sql_service_filter = warp::any().map(move || sql_service.clone());

    let query_route = warp::path!("query")
        .and(warp::body::json())
        .and(sql_service_filter.clone())
        .and_then(post_query);

    // let import_route = warp::path!("insert" / String)
    //     .and(warp::body::aggregate())
    //     .and_then(post_insert);

    warp::serve(
        query_route, // .or(import_route)
    )
    .run(([127, 0, 0, 1], 3030))
    .await;

    Ok(())
}

// curl -X POST  -d '{"query":"create schema boo"}' -H "Content-Type: application/json" http://127.0.0.1:3030/query
pub async fn post_query(
    query_body: SqlQueryBody,
    sql_service: Arc<dyn SqlService>,
) -> Result<String, Rejection> {
    let res = sql_service.exec_query(&query_body.query).await?;
    debug!("Query result is {:?}", res);
    debug!("Post query: {:?}", query_body);
    Ok(format!("{:?}", res))
}
