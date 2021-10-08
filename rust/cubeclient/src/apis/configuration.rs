use reqwest::{self};
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};

#[derive(Debug, Clone)]
pub struct ConfigurationImpl<T> {
    pub client: T,
    pub base_path: String,
    pub user_agent: Option<String>,
    pub basic_auth: Option<BasicAuth>,
    pub oauth_access_token: Option<String>,
    pub bearer_access_token: Option<String>,
    pub api_key: Option<ApiKey>,
    // TODO: take an oauth2 token source, similar to the go one
}

pub type Configuration = ConfigurationImpl<ClientWithMiddleware>;

pub type BasicAuth = (String, Option<String>);

#[derive(Debug, Clone)]
pub struct ApiKey {
    pub prefix: Option<String>,
    pub key: String,
}

impl Configuration {
    pub fn new(client: ClientWithMiddleware) -> Configuration {
        Configuration {
            client,
            base_path: "http://localhost".to_owned(),
            user_agent: Some("CubeClient/1.0.0/rust".to_owned()),
            basic_auth: None,
            oauth_access_token: None,
            bearer_access_token: None,
            api_key: None,
        }
    }
}

impl Default for Configuration {
    fn default() -> Self {
        let client = ClientBuilder::new(reqwest::Client::new()).build();

        Configuration::new(client)
    }
}

impl<T> From<reqwest_middleware::Error> for crate::apis::Error<T> {
    fn from(v: reqwest_middleware::Error) -> Self {
        match v {
            reqwest_middleware::Error::Middleware(m) => crate::apis::Error::Middleware(m),
            reqwest_middleware::Error::Reqwest(e) => crate::apis::Error::Reqwest(e),
        }
    }
}
