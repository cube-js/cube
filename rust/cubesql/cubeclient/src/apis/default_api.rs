use log::{debug, error};
use reqwest;
use uuid::Uuid;

use super::{configuration, Error};
use crate::apis::ResponseContent;

/// struct for typed errors of method [`load_v1`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LoadV1Error {
    Status4XX(crate::models::V1Error),
    Status5XX(crate::models::V1Error),
    UnknownValue(serde_json::Value),
}

/// struct for typed errors of method [`meta_v1`]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetaV1Error {
    Status4XX(crate::models::V1Error),
    Status5XX(crate::models::V1Error),
    UnknownValue(serde_json::Value),
}

pub async fn load_v1(
    configuration: &configuration::Configuration,
    v1_load_request: Option<crate::models::V1LoadRequest>,
) -> Result<crate::models::V1LoadResponse, Error<LoadV1Error>> {
    let local_var_client = &configuration.client;

    let request_id = Uuid::new_v4().to_string();
    let mut span_counter: u32 = 1;

    loop {
        let local_var_uri_str = format!("{}/v1/load", configuration.base_path);
        let mut local_var_req_builder =
            local_var_client.request(reqwest::Method::POST, local_var_uri_str.as_str());

        if let Some(ref local_var_user_agent) = configuration.user_agent {
            local_var_req_builder = local_var_req_builder
                .header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
        }

        if let Some(ref local_var_token) = configuration.bearer_access_token {
            local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
        };
        local_var_req_builder = local_var_req_builder.json(&v1_load_request);

        local_var_req_builder = local_var_req_builder.header(
            "x-request-id",
            format!("{}-span-{}", request_id, span_counter),
        );

        let local_var_req = local_var_req_builder.build()?;
        let local_var_resp = local_var_client.execute(local_var_req).await?;

        let local_var_status = local_var_resp.status();
        let local_var_content = local_var_resp.text().await?;

        if !local_var_status.is_client_error() && !local_var_status.is_server_error() {
            let response_ok =
                serde_json::from_str::<crate::models::V1LoadResponse>(&local_var_content)
                    .map_err(Error::from);
            if response_ok.is_ok() {
                return response_ok;
            };

            let response_err =
                serde_json::from_str::<crate::models::V1LoadContinueWait>(&local_var_content);
            if let Ok(res) = response_err {
                if res.error.to_lowercase() == *"continue wait" {
                    debug!(
                        "[client] load - retrying request (continue wait) requestId: {}, span: {}",
                        request_id, span_counter
                    );

                    span_counter += 1;

                    continue;
                } else {
                    error!(
                        "[client] load - strange response, success which contains error: {:?}",
                        res
                    );

                    let local_var_error = ResponseContent {
                        status: local_var_status,
                        content: local_var_content,
                        entity: None,
                    };

                    return Err(Error::ResponseError(local_var_error));
                }
            };

            return response_ok;
        };

        let local_var_entity: Option<LoadV1Error> = serde_json::from_str(&local_var_content).ok();
        let local_var_error = ResponseContent {
            status: local_var_status,
            content: local_var_content,
            entity: local_var_entity,
        };

        return Err(Error::ResponseError(local_var_error));
    }
}

pub async fn meta_v1(
    configuration: &configuration::Configuration,
) -> Result<crate::models::V1MetaResponse, Error<MetaV1Error>> {
    let local_var_configuration = configuration;

    let local_var_client = &local_var_configuration.client;

    let local_var_uri_str = format!("{}/v1/meta", local_var_configuration.base_path);
    let mut local_var_req_builder =
        local_var_client.request(reqwest::Method::GET, local_var_uri_str.as_str());

    let request_id = Uuid::new_v4().to_string();
    local_var_req_builder = local_var_req_builder.header("x-request-id", request_id + "-span-1");

    if let Some(ref local_var_user_agent) = local_var_configuration.user_agent {
        local_var_req_builder =
            local_var_req_builder.header(reqwest::header::USER_AGENT, local_var_user_agent.clone());
    }
    if let Some(ref local_var_token) = local_var_configuration.bearer_access_token {
        local_var_req_builder = local_var_req_builder.bearer_auth(local_var_token.to_owned());
    };

    let local_var_req = local_var_req_builder.build()?;
    let local_var_resp = local_var_client.execute(local_var_req).await?;

    let local_var_status = local_var_resp.status();
    let local_var_content = local_var_resp.text().await?;

    if !local_var_status.is_client_error() && !local_var_status.is_server_error() {
        serde_json::from_str(&local_var_content).map_err(Error::from)
    } else {
        let local_var_entity: Option<MetaV1Error> = serde_json::from_str(&local_var_content).ok();
        let local_var_error = ResponseContent {
            status: local_var_status,
            content: local_var_content,
            entity: local_var_entity,
        };
        Err(Error::ResponseError(local_var_error))
    }
}

#[cfg(test)]
mod tests {
    use reqwest::Client;
    use reqwest_middleware::ClientBuilder;
    use std::sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    };
    use wiremock::{
        matchers::{method, path},
        Mock, MockServer, Respond, ResponseTemplate,
    };

    use crate::apis::configuration::Configuration;

    use super::*;

    pub struct TestResponder(Arc<AtomicU32>, u32, std::time::Duration);

    impl TestResponder {
        fn new(retries_to_resolve: u32, initial_timeout: std::time::Duration) -> Self {
            Self(
                Arc::new(AtomicU32::new(0)),
                retries_to_resolve,
                initial_timeout,
            )
        }
    }

    impl Respond for TestResponder {
        fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
            let mut retries = self.0.load(Ordering::SeqCst);
            retries += 1;
            self.0.store(retries, Ordering::SeqCst);

            if retries + 1 >= self.1 {
                ResponseTemplate::new(200).set_body_string(
                    r#"{
                        "queryType": "regularQuery",
                        "results": [
                            {
                                "annotation": {
                                    "measures": {},
                                    "dimensions": {},
                                    "segments": {},
                                    "timeDimensions": {}
                                },
                                "data": []
                            }
                        ]
                    }"#,
                )
            } else {
                ResponseTemplate::new(200)
                    .set_delay(self.2)
                    .set_body_string(r#"{"error":"Continue Wait"}"#)
            }
        }
    }

    #[tokio::test]
    async fn test_continue_wait() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/load"))
            .respond_with(TestResponder::new(
                // 5 retries is needed to complete this timeout
                5,
                // after 5 seconds we return Continue Wait
                std::time::Duration::from_millis(500),
            ))
            .expect(4)
            .mount(&server)
            .await;

        let reqwest_client = Client::builder().build().unwrap();
        let client = ClientBuilder::new(reqwest_client).build();

        let mut configuration = Configuration::new(client);
        configuration.base_path = server.uri();

        let resp = load_v1(&configuration, None).await;
        match resp {
            Ok(_) => {}
            Err(e) => panic!("must be successful, {:?}", e),
        };
    }
}
