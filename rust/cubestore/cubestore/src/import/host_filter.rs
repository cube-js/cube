use crate::CubeError;
use regex::Regex;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use url::{Host, Url};

/// Allowlist for `http(s)://` import hosts (see `CUBESTORE_LOCATION_ALLOW_HOSTS` docs).
/// A lone `*` (the default) allows every host; with no entries no host is allowed.
/// `deny_private_ips` also refuses non-public addresses, for host names after DNS resolution.
#[derive(Debug, Clone)]
pub struct LocationHostFilter {
    patterns: Vec<HostPattern>,
    deny_private_ips: bool,
}

impl Default for LocationHostFilter {
    fn default() -> Self {
        Self {
            patterns: vec![HostPattern::Any],
            deny_private_ips: false,
        }
    }
}

#[derive(Debug, Clone)]
enum HostPattern {
    Any,
    Exact(String),
    Wildcard(Regex),
    /// Kept parsed: the URL parser canonicalizes IP hosts, so entry text may never equal them.
    Ip {
        ip: IpAddr,
        port: Option<u16>,
    },
    Subnet(IpSubnet),
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

/// Addresses that are not publicly routable: loopback, RFC 1918, CGNAT, link-local (cloud
/// metadata endpoints), benchmarking, multicast, reserved, unique local IPv6.
fn is_non_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_non_public_ipv4(ip),
        IpAddr::V6(ip) => {
            if let Some(v4) = ip.to_ipv4_mapped() {
                return is_non_public_ipv4(v4);
            }
            let segments = ip.segments();
            // NAT64 (64:ff9b::/96) reaches the embedded IPv4 address.
            if segments[..6] == [0x64, 0xff9b, 0, 0, 0, 0] {
                let [.., hi, lo] = segments;
                return is_non_public_ipv4(Ipv4Addr::from(((hi as u32) << 16) | lo as u32));
            }
            ip.is_unspecified()
                || ip.is_loopback()
                || ip.is_multicast()
                // fc00::/7 unique local, fe80::/10 link-local.
                || (segments[0] & 0xfe00) == 0xfc00
                || (segments[0] & 0xffc0) == 0xfe80
                // ::/96 deprecated IPv4-compatible addresses.
                || ip.octets()[..12] == [0u8; 12]
        }
    }
}

fn is_non_public_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    ip.is_unspecified()
        || a == 0
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        // 100.64.0.0/10 carrier-grade NAT.
        || (a == 100 && (b & 0xc0) == 64)
        // 192.0.0.0/24 IETF protocol assignments.
        || (a == 192 && b == 0 && c == 0)
        // 198.18.0.0/15 benchmarking.
        || (a == 198 && (b & 0xfe) == 18)
        // 240.0.0.0/4 reserved.
        || a >= 240
}

impl LocationHostFilter {
    pub fn new(entries: &[String], deny_private_ips: bool) -> Result<Self, CubeError> {
        let patterns = entries
            .iter()
            .map(|e| Self::parse_entry(e))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            patterns,
            deny_private_ips,
        })
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
        if entry == "*" {
            return Ok(HostPattern::Any);
        }
        if let Some((ip, port)) = Self::parse_ip_entry(&entry) {
            return Ok(HostPattern::Ip {
                ip: Self::unmap(ip),
                port,
            });
        }
        let (host, port) = match entry.rsplit_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (entry.as_str(), None),
        };
        let is_host_char =
            |c: char| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '*');
        if host.is_empty()
            || !host.chars().all(is_host_char)
            || port.map_or(false, |p| p.parse::<u16>().is_err())
        {
            return Err(CubeError::user(format!(
                "Invalid allowed host {:?}: expected `host`, `host:port`, a pattern like `*.example.com`, an IP address (IPv6 with a port as `[::1]:8080`) or a subnet like `10.0.0.0/8`",
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

    /// `127.0.0.1`, `127.0.0.1:9000`, `::1`, `[::1]`, `[::1]:9000`.
    fn parse_ip_entry(entry: &str) -> Option<(IpAddr, Option<u16>)> {
        fn bracketed(s: &str) -> Option<IpAddr> {
            let v6 = s.strip_prefix('[')?.strip_suffix(']')?;
            v6.parse::<Ipv6Addr>().ok().map(IpAddr::V6)
        }
        if let Some(ip) = bracketed(entry).or_else(|| entry.parse().ok()) {
            return Some((ip, None));
        }
        let (host, port) = entry.rsplit_once(':')?;
        let ip = bracketed(host).or_else(|| host.parse::<Ipv4Addr>().ok().map(IpAddr::V4))?;
        Some((ip, Some(port.parse().ok()?)))
    }

    pub fn is_unrestricted(&self) -> bool {
        !self.deny_private_ips && self.allows_any_host()
    }

    fn allows_any_host(&self) -> bool {
        self.patterns.iter().any(|p| matches!(p, HostPattern::Any))
    }

    /// Whether the connection may go to `ip`, whichever host name led there.
    pub fn is_ip_permitted(&self, ip: IpAddr) -> bool {
        !self.deny_private_ips || !is_non_public_ip(ip) || self.ip_is_listed(ip)
    }

    /// Covered by a subnet entry, or by an IP entry (on any port).
    fn ip_is_listed(&self, ip: IpAddr) -> bool {
        let ip = Self::unmap(ip);
        self.patterns.iter().any(|p| match p {
            HostPattern::Subnet(subnet) => subnet.contains(ip),
            HostPattern::Ip { ip: entry, .. } => *entry == ip,
            HostPattern::Exact(_) | HostPattern::Wildcard(_) | HostPattern::Any => false,
        })
    }

    fn unmap(ip: IpAddr) -> IpAddr {
        match ip {
            IpAddr::V6(v6) => v6.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(ip),
            v4 => v4,
        }
    }

    pub fn is_allowed(&self, url: &Url) -> bool {
        let ip = Self::ip_of(url);
        if let Some(ip) = ip {
            if !self.is_ip_permitted(ip) {
                return false;
            }
        }
        if self.allows_any_host() {
            return true;
        }
        let Some(host) = url.host_str() else {
            return false;
        };
        let host = host.to_lowercase();
        let port = url.port_or_known_default();
        let host_port = port.map(|port| format!("{}:{}", host, port));
        self.patterns.iter().any(|p| match p {
            HostPattern::Any => true,
            HostPattern::Subnet(subnet) => ip.map_or(false, |ip| subnet.contains(ip)),
            HostPattern::Ip {
                ip: entry,
                port: entry_port,
            } => ip == Some(*entry) && entry_port.map_or(true, |p| Some(p) == port),
            HostPattern::Exact(entry) => *entry == host || Some(entry) == host_port.as_ref(),
            HostPattern::Wildcard(regex) => {
                regex.is_match(&host) || host_port.as_ref().map_or(false, |c| regex.is_match(c))
            }
        })
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
        if let Some(ip) = Self::ip_of(&url) {
            if !self.is_ip_permitted(ip) {
                return Err(CubeError::user(format!(
                    "Import location '{}' points to a non-public IP address, which CUBESTORE_LOCATION_DENY_PRIVATE_IPS forbids. Add its subnet to CUBESTORE_LOCATION_ALLOW_HOSTS to allow it.",
                    location
                )));
            }
        }
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
        if self.deny_private_ips {
            builder = builder
                .dns_resolver(Arc::new(PermittedIpResolver {
                    filter: self.clone(),
                }))
                // A proxy would resolve the host itself, out of the resolver's reach.
                .no_proxy();
        }
        Ok(builder.build()?)
    }
}

/// Resolves host names and keeps only the addresses `LocationHostFilter` permits, so the
/// connection is made to an address that was actually checked.
struct PermittedIpResolver {
    filter: LocationHostFilter,
}

impl reqwest::dns::Resolve for PermittedIpResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let filter = self.filter.clone();
        let host = name.as_str().to_string();
        Box::pin(async move {
            let resolved: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            let permitted: Vec<SocketAddr> = resolved
                .iter()
                .copied()
                .filter(|addr| filter.is_ip_permitted(addr.ip()))
                .collect();
            if permitted.is_empty() {
                let denied = resolved
                    .iter()
                    .map(|a| a.ip().to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(format!(
                    "{} resolves only to non-public IP addresses ({}), which CUBESTORE_LOCATION_DENY_PRIVATE_IPS forbids",
                    host, denied
                )
                .into());
            }
            let addrs: reqwest::dns::Addrs = Box::new(permitted.into_iter());
            Ok(addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::LocationHostFilter;

    fn filter(entries: &[&str]) -> LocationHostFilter {
        LocationHostFilter::new(
            &entries.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            false,
        )
        .unwrap()
    }

    #[test]
    fn lone_star_allows_everything() {
        for f in [filter(&["*"]), LocationHostFilter::default()] {
            assert!(f.is_unrestricted());
            assert!(f.check("http://127.0.0.1:8080/data.csv").is_ok());
            assert!(f.check("https://a.b.anything.example/data.csv").is_ok());
        }
    }

    #[test]
    fn empty_list_allows_nothing() {
        let f = filter(&[]);
        assert!(!f.is_unrestricted());
        assert!(f.check("http://127.0.0.1:8080/data.csv").is_err());
        assert!(f.check("https://anything.example/data.csv").is_err());
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

    fn deny_private(entries: &[&str]) -> LocationHostFilter {
        LocationHostFilter::new(
            &entries.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            true,
        )
        .unwrap()
    }

    #[test]
    fn deny_private_ips_rejects_non_public_literals() {
        let f = deny_private(&["*"]);
        assert!(f.check("https://example.com/a.csv").is_ok());
        assert!(f.check("http://8.8.8.8/a.csv").is_ok());
        assert!(f.check("http://[2606:4700::1111]/a.csv").is_ok());
        for location in [
            "http://127.0.0.1/a.csv",
            "http://0.0.0.0/a.csv",
            "http://10.1.2.3/a.csv",
            "http://172.16.0.1/a.csv",
            "http://192.168.0.1/a.csv",
            "http://169.254.169.254/latest/meta-data",
            "http://100.64.0.1/a.csv",
            "http://198.18.0.1/a.csv",
            "http://255.255.255.255/a.csv",
            "http://0x7f000001/a.csv",
            "http://[::1]/a.csv",
            "http://[::]/a.csv",
            "http://[fd00::1]/a.csv",
            "http://[fe80::1]/a.csv",
            "http://[::ffff:127.0.0.1]/a.csv",
            "http://[::ffff:169.254.169.254]/a.csv",
            "http://[64:ff9b::a9fe:a9fe]/a.csv",
        ] {
            let err = f.check(location).unwrap_err().message;
            assert!(
                err.contains("CUBESTORE_LOCATION_DENY_PRIVATE_IPS"),
                "{}: {}",
                location,
                err
            );
        }
    }

    #[test]
    fn deny_private_ips_keeps_listed_subnets_and_ips() {
        let f = deny_private(&["10.0.0.0/8", "127.0.0.1:9000", "*.example.com"]);
        assert!(f.check("http://10.1.2.3/a.csv").is_ok());
        assert!(f.check("http://127.0.0.1:9000/a.csv").is_ok());
        // The IP is listed, but only on port 9000.
        assert!(f.check("http://127.0.0.1:9001/a.csv").is_err());
        assert!(f.check("http://192.168.0.1/a.csv").is_err());
        assert!(f.is_ip_permitted("10.0.0.5".parse().unwrap()));
        assert!(f.is_ip_permitted("127.0.0.1".parse().unwrap()));
        assert!(f.is_ip_permitted("::ffff:10.0.0.5".parse().unwrap()));
        assert!(!f.is_ip_permitted("192.168.0.1".parse().unwrap()));
        assert!(f.is_ip_permitted("93.184.215.14".parse().unwrap()));
    }

    #[test]
    fn deny_private_ips_off_by_default() {
        let f = filter(&["10.0.0.0/8"]);
        assert!(f.is_ip_permitted("127.0.0.1".parse().unwrap()));
    }

    #[test]
    fn ipv6_hosts_are_bracketed() {
        let f = filter(&["[::1]"]);
        assert!(f.check("http://[::1]:8080/a.csv").is_ok());
        assert!(f.check("http://[::2]/a.csv").is_err());
    }

    #[test]
    fn ip_entries_match_any_spelling() {
        for entry in ["::1", "[::1]", "[0:0:0:0:0:0:0:1]", "0:0:0:0:0:0:0:1"] {
            let f = filter(&[entry]);
            assert!(f.check("http://[::1]:8080/a.csv").is_ok(), "{}", entry);
            assert!(f.check("http://[0:0::1]/a.csv").is_ok(), "{}", entry);
            assert!(f.check("http://[::2]/a.csv").is_err(), "{}", entry);
        }
        let f = filter(&["[FD00:0::1]:9000", "::ffff:10.0.0.1", "127.0.0.1:9000"]);
        assert!(f.check("http://[fd00::1]:9000/a.csv").is_ok());
        assert!(f.check("http://[fd00::1]:9001/a.csv").is_err());
        assert!(f.check("http://[fd00::1]/a.csv").is_err());
        assert!(f.check("http://10.0.0.1/a.csv").is_ok());
        assert!(f.check("http://0x7f000001:9000/a.csv").is_ok());
        assert!(f.check("http://127.0.0.1/a.csv").is_err());

        let f = deny_private(&["fd00::1"]);
        assert!(f.check("http://[fd00::1]/a.csv").is_ok());
        assert!(f.is_ip_permitted("fd00::1".parse().unwrap()));
        assert!(!f.is_ip_permitted("fd00::2".parse().unwrap()));
    }

    #[test]
    fn invalid_entries_are_rejected() {
        for entry in [
            "https://example.com",
            "example.com/path",
            " ",
            r"files\d+\.com",
            "a b",
            "host:port",
            "127.0.0.1:99999",
            "::zz",
            "[::1",
            "[127.0.0.1]",
            "::1:x",
            "fd00::1::2",
            "a:b:c",
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
        let filter = LocationHostFilter::new(&["127.0.0.1".to_string()], false).unwrap();
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

#[cfg(test)]
mod resolver_tests {
    use super::LocationHostFilter;

    fn client(entries: &[&str]) -> reqwest::Client {
        LocationHostFilter::new(
            &entries.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            true,
        )
        .unwrap()
        .http_client()
        .unwrap()
    }

    #[tokio::test]
    async fn hostnames_resolving_to_private_ips_are_refused() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let err = client(&["*"])
            .get(format!("http://localhost:{}/a.csv", port))
            .send()
            .await
            .unwrap_err();
        let message = format!("{:?}", err);
        assert!(
            message.contains("CUBESTORE_LOCATION_DENY_PRIVATE_IPS"),
            "unexpected error: {}",
            message
        );
    }

    #[tokio::test]
    async fn hostnames_resolving_to_listed_subnets_connect() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = socket.read(&mut buf).await;
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                    )
                    .await;
            }
        });
        let res = client(&["127.0.0.0/8"])
            .get(format!("http://localhost:{}/a.csv", port))
            .send()
            .await
            .unwrap();
        assert_eq!(res.text().await.unwrap(), "ok");
    }
}
