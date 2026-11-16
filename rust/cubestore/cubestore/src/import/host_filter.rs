use crate::CubeError;
use regex::Regex;
use std::net::IpAddr;
use url::{Host, Url};

/// Restricts the hosts `http(s)://` import locations may point to, modeled on ClickHouse's
/// `remote_url_allow_hosts`. A `host` entry matches the host on any port, a `host:port` entry
/// matches that port only. `*` stands for one domain label, so `*.example.com` matches
/// `a.example.com` but neither `a.b.example.com` nor `example.com`. A CIDR entry such as
/// `10.0.0.0/8` matches IP literals in that subnet on any port; hostnames are not resolved.
/// With no entries every host is allowed, so existing deployments keep working.
#[derive(Debug, Clone, Default)]
pub struct LocationHostFilter {
    patterns: Vec<HostPattern>,
}

#[derive(Debug, Clone)]
enum HostPattern {
    Exact(String),
    Wildcard(Regex),
    Subnet(IpSubnet),
}

impl HostPattern {
    fn matches(&self, candidate: &str) -> bool {
        match self {
            HostPattern::Exact(host) => host == candidate,
            HostPattern::Wildcard(regex) => regex.is_match(candidate),
            HostPattern::Subnet(_) => false,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct IpSubnet {
    network: IpAddr,
    prefix: u8,
}

impl IpSubnet {
    fn parse(entry: &str) -> Option<Result<Self, String>> {
        let (addr, prefix) = entry.split_once('/')?;
        let addr = addr.trim_start_matches('[').trim_end_matches(']');
        let Ok(network) = addr.parse::<IpAddr>() else {
            return Some(Err(format!("{:?} is not an IP address", addr)));
        };
        let max_prefix = if network.is_ipv4() { 32 } else { 128 };
        let prefix = match prefix.parse::<u8>() {
            Ok(p) if p <= max_prefix => p,
            _ => {
                return Some(Err(format!(
                    "prefix length must be between 0 and {}",
                    max_prefix
                )))
            }
        };
        Some(Ok(Self { network, prefix }))
    }

    fn contains(&self, ip: IpAddr) -> bool {
        fn masked(bits: u128, width: u8, prefix: u8) -> u128 {
            if prefix == 0 {
                0
            } else {
                bits >> (width - prefix)
            }
        }
        match (self.network, ip) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                masked(u32::from(net) as u128, 32, self.prefix)
                    == masked(u32::from(ip) as u128, 32, self.prefix)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                masked(u128::from(net), 128, self.prefix)
                    == masked(u128::from(ip), 128, self.prefix)
            }
            _ => false,
        }
    }
}

impl LocationHostFilter {
    pub fn new(entries: &[String]) -> Result<Self, CubeError> {
        let patterns = entries
            .iter()
            .map(|e| Self::parse_entry(e))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { patterns })
    }

    /// Validates one allowlist entry, e.g. `files.example.com`, `*.example.com:8443`, `10.0.0.0/8`.
    pub fn validate_entry(entry: &str) -> Result<String, CubeError> {
        Self::parse_entry(entry)?;
        Ok(entry.trim().to_lowercase())
    }

    fn parse_entry(entry: &str) -> Result<HostPattern, CubeError> {
        let entry = entry.trim().to_lowercase();
        if let Some(subnet) = IpSubnet::parse(&entry) {
            return subnet.map(HostPattern::Subnet).map_err(|e| {
                CubeError::user(format!("Invalid allowed subnet {:?}: {}", entry, e))
            });
        }
        let is_host_char =
            |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | ':' | '[' | ']');
        if entry.is_empty() || !entry.chars().all(|c| c == '*' || is_host_char(c)) {
            return Err(CubeError::user(format!(
                "Invalid allowed host {:?}: expected `host`, `host:port`, a pattern like `*.example.com` or a subnet like `10.0.0.0/8`",
                entry
            )));
        }
        if !entry.contains('*') {
            return Ok(HostPattern::Exact(entry));
        }
        let pattern = entry
            .split('*')
            .map(regex::escape)
            .collect::<Vec<_>>()
            .join("[^.:]*");
        // Anchored so `*.example.com` cannot be satisfied by `a.example.com.attacker.io`.
        let regex = Regex::new(&format!("^{}$", pattern))
            .map_err(|e| CubeError::user(format!("Invalid allowed host {:?}: {}", entry, e)))?;
        Ok(HostPattern::Wildcard(regex))
    }

    pub fn is_unrestricted(&self) -> bool {
        self.patterns.is_empty()
    }

    pub fn is_allowed(&self, url: &Url) -> bool {
        if self.is_unrestricted() {
            return true;
        }
        let Some(host) = url.host_str() else {
            return false;
        };
        if let Some(ip) = Self::ip_of(url) {
            if self.patterns.iter().any(|p| match p {
                HostPattern::Subnet(subnet) => subnet.contains(ip),
                _ => false,
            }) {
                return true;
            }
        }
        let host = host.to_lowercase();
        let host_port = url
            .port_or_known_default()
            .map(|port| format!("{}:{}", host, port));
        std::iter::once(&host)
            .chain(host_port.as_ref())
            .any(|candidate| self.patterns.iter().any(|p| p.matches(candidate)))
    }

    /// IPv4-mapped IPv6 literals (`[::ffff:10.0.0.1]`) count as their IPv4 address.
    fn ip_of(url: &Url) -> Option<IpAddr> {
        match url.host()? {
            Host::Ipv4(ip) => Some(IpAddr::V4(ip)),
            Host::Ipv6(ip) => Some(
                ip.to_ipv4_mapped()
                    .map(IpAddr::V4)
                    .unwrap_or(IpAddr::V6(ip)),
            ),
            Host::Domain(_) => None,
        }
    }

    pub fn check(&self, location: &str) -> Result<(), CubeError> {
        let url = Url::parse(location).map_err(|e| {
            CubeError::user(format!(
                "Invalid http import location '{}': {}",
                location, e
            ))
        })?;
        if self.is_allowed(&url) {
            Ok(())
        } else {
            Err(CubeError::user(format!(
                "Host of import location '{}' is not allowed. Add it to CUBESTORE_LOCATION_ALLOW_HOSTS.",
                location
            )))
        }
    }

    /// An HTTP client whose redirects are held to the same allowlist as the location itself.
    pub fn http_client(&self) -> Result<reqwest::Client, CubeError> {
        let mut builder = reqwest::Client::builder();
        if !self.is_unrestricted() {
            let filter = self.clone();
            builder = builder.redirect(reqwest::redirect::Policy::custom(move |attempt| {
                if attempt.previous().len() >= 10 {
                    attempt.error("too many redirects")
                } else if filter.is_allowed(attempt.url()) {
                    attempt.follow()
                } else {
                    let message = format!(
                        "redirect to a host that is not allowed: {}",
                        attempt.url().host_str().unwrap_or("<none>")
                    );
                    attempt.error(message)
                }
            }));
        }
        Ok(builder.build()?)
    }
}

#[cfg(test)]
mod tests {
    use super::LocationHostFilter;

    fn filter(entries: &[&str]) -> LocationHostFilter {
        LocationHostFilter::new(&entries.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap()
    }

    #[test]
    fn empty_filter_allows_everything() {
        let f = filter(&[]);
        assert!(f.check("http://127.0.0.1:8080/data.csv").is_ok());
        assert!(f.check("https://anything.example/data.csv").is_ok());
    }

    #[test]
    fn exact_host_matches_any_port() {
        let f = filter(&["data.example.com"]);
        assert!(f.check("https://data.example.com/a.csv").is_ok());
        assert!(f.check("http://DATA.example.com:8080/a.csv").is_ok());
        assert!(f.check("https://other.example.com/a.csv").is_err());
        assert!(f.check("https://data.example.com.evil.io/a.csv").is_err());
        assert!(f.check("https://sub.data.example.com/a.csv").is_err());
    }

    #[test]
    fn host_with_port_matches_that_port_only() {
        let f = filter(&["data.example.com:8443", "plain.example.com:443"]);
        assert!(f.check("https://data.example.com:8443/a.csv").is_ok());
        assert!(f.check("https://data.example.com/a.csv").is_err());
        // The scheme's default port counts as the URL's port.
        assert!(f.check("https://plain.example.com/a.csv").is_ok());
        assert!(f.check("http://plain.example.com/a.csv").is_err());
    }

    #[test]
    fn wildcard_matches_one_label() {
        let f = filter(&["*.myhost.ru"]);
        assert!(f.check("https://files.myhost.ru/a.csv").is_ok());
        assert!(f.check("http://FILES.MYHOST.RU:8080/a.csv").is_ok());
        assert!(f.check("https://myhost.ru/a.csv").is_err());
        assert!(f.check("https://a.b.myhost.ru/a.csv").is_err());
        assert!(f.check("https://files.myhost.ru.evil.io/a.csv").is_err());
        assert!(f.check("https://evilmyhost.ru/a.csv").is_err());
        assert!(f.check("https://evil.io/files.myhost.ru").is_err());
    }

    #[test]
    fn wildcard_inside_a_label_and_with_port() {
        let f = filter(&["files-*.example.com", "*.internal:9000"]);
        assert!(f.check("https://files-eu.example.com/a.csv").is_ok());
        assert!(f.check("https://data-eu.example.com/a.csv").is_err());
        assert!(f.check("http://minio.internal:9000/a.csv").is_ok());
        assert!(f.check("http://minio.internal:9001/a.csv").is_err());
        assert!(f.check("http://minio.internal/a.csv").is_err());
    }

    #[test]
    fn wildcard_does_not_cross_into_the_port() {
        let f = filter(&["minio.*"]);
        assert!(f.check("http://minio.local:9000/a.csv").is_ok());
        assert!(f.check("http://minio.local.evil.io/a.csv").is_err());
    }

    #[test]
    fn exact_hosts_and_patterns_combine() {
        let f = filter(&["127.0.0.1", "*.s3.amazonaws.com"]);
        assert!(f.check("http://127.0.0.1:19099/a.csv").is_ok());
        assert!(f
            .check("https://bucket.s3.amazonaws.com/a.csv?X-Amz-Signature=1")
            .is_ok());
        assert!(f.check("http://169.254.169.254/latest/meta-data").is_err());
    }

    #[test]
    fn ipv4_subnets_match_ip_literals() {
        let f = filter(&["10.0.0.0/8", "192.168.1.0/24"]);
        assert!(f.check("http://10.1.2.3:9000/a.csv").is_ok());
        assert!(f.check("http://192.168.1.200/a.csv").is_ok());
        assert!(f.check("http://192.168.2.1/a.csv").is_err());
        assert!(f.check("http://11.0.0.1/a.csv").is_err());
        assert!(f.check("http://169.254.169.254/latest/meta-data").is_err());
        // Alternative IPv4 spellings are normalized by the URL parser.
        assert!(f.check("http://0x0a000001/a.csv").is_ok());
        assert!(f.check("http://[::ffff:10.0.0.1]/a.csv").is_ok());
        // Hostnames are not resolved.
        assert!(f.check("http://10.0.0.1.nip.io/a.csv").is_err());
    }

    #[test]
    fn subnet_edges() {
        let f = filter(&["10.0.0.5/32"]);
        assert!(f.check("http://10.0.0.5/a.csv").is_ok());
        assert!(f.check("http://10.0.0.6/a.csv").is_err());
        let any = filter(&["0.0.0.0/0"]);
        assert!(any.check("http://8.8.8.8/a.csv").is_ok());
        assert!(any.check("http://[::1]/a.csv").is_err());
        assert!(any.check("https://example.com/a.csv").is_err());
    }

    #[test]
    fn ipv6_subnets_match_ip_literals() {
        let f = filter(&["fd00::/8", "[2001:db8::]/32"]);
        assert!(f.check("http://[fd12:3456::1]:8080/a.csv").is_ok());
        assert!(f.check("http://[2001:db8:1::1]/a.csv").is_ok());
        assert!(f.check("http://[fe80::1]/a.csv").is_err());
        assert!(f.check("http://10.0.0.1/a.csv").is_err());
    }

    #[test]
    fn invalid_subnets_are_rejected() {
        for entry in [
            "10.0.0.0/33",
            "fd00::/129",
            "10.0.0.0/",
            "example.com/8",
            "10.0.0.0/x",
        ] {
            assert!(
                LocationHostFilter::validate_entry(entry).is_err(),
                "{:?} should be rejected",
                entry
            );
        }
    }

    #[test]
    fn ipv6_hosts_are_bracketed() {
        let f = filter(&["[::1]"]);
        assert!(f.check("http://[::1]:8080/a.csv").is_ok());
        assert!(f.check("http://[::2]/a.csv").is_err());
    }

    #[test]
    fn invalid_entries_are_rejected() {
        for entry in [
            "https://example.com",
            "example.com/path",
            " ",
            r"files\d+\.com",
            "a b",
        ] {
            assert!(
                LocationHostFilter::validate_entry(entry).is_err(),
                "{:?} should be rejected",
                entry
            );
        }
        assert_eq!(
            LocationHostFilter::validate_entry(" *.MyHost.ru ").unwrap(),
            "*.myhost.ru"
        );
    }
}

#[cfg(test)]
mod redirect_tests {
    use super::LocationHostFilter;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Answers every request with a redirect to `localhost` on the same port.
    async fn redirecting_server() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let response = format!(
                    "HTTP/1.1 302 Found\r\nLocation: http://localhost:{}/b.csv\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    port
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        port
    }

    #[tokio::test]
    async fn redirects_to_disallowed_hosts_are_refused() {
        let port = redirecting_server().await;
        let filter = LocationHostFilter::new(&["127.0.0.1".to_string()]).unwrap();
        let err = filter
            .http_client()
            .unwrap()
            .get(format!("http://127.0.0.1:{}/a.csv", port))
            .send()
            .await
            .unwrap_err();
        assert!(err.is_redirect(), "unexpected error: {}", err);
    }
}
