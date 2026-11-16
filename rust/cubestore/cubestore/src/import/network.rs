use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::redirect;
use url::{Host, Url};

use crate::CubeError;

const MAX_REDIRECTS: usize = 10;

pub const ALLOW_PRIVATE_NETWORK_ENV: &str = "CUBESTORE_LOCATION_ALLOW_PRIVATE_NETWORK";

/// Whether `ip` is outside the publicly routable address space: loopback, private,
/// link-local (including cloud metadata endpoints), shared, reserved and similar ranges.
pub fn is_private_network_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_private_network_ipv4(ip),
        IpAddr::V6(ip) => is_private_network_ipv6(ip),
    }
}

fn is_private_network_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_multicast()
        // 0.0.0.0/8 "this network"
        || a == 0
        // 100.64.0.0/10 shared address space (carrier-grade NAT)
        || (a == 100 && (b & 0xc0) == 64)
        // 192.0.0.0/24 IETF protocol assignments
        || (a == 192 && b == 0 && c == 0)
        // 198.18.0.0/15 benchmarking
        || (a == 198 && (b & 0xfe) == 18)
        // 240.0.0.0/4 reserved
        || a >= 240
}

fn is_private_network_ipv6(ip: Ipv6Addr) -> bool {
    if let Some(v4) = ip.to_ipv4_mapped() {
        return is_private_network_ipv4(v4);
    }
    let s = ip.segments();
    let embedded_v4 =
        |hi: u16, lo: u16| Ipv4Addr::new((hi >> 8) as u8, hi as u8, (lo >> 8) as u8, lo as u8);
    ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_multicast()
        // fc00::/7 unique local
        || (s[0] & 0xfe00) == 0xfc00
        // fe80::/10 link-local
        || (s[0] & 0xffc0) == 0xfe80
        // fec0::/10 site-local (deprecated, still routed by some stacks)
        || (s[0] & 0xffc0) == 0xfec0
        // 2001:db8::/32 documentation
        || (s[0] == 0x2001 && s[1] == 0x0db8)
        // 64:ff9b:1::/48 local-use NAT64
        || (s[0] == 0x0064 && s[1] == 0xff9b && s[2] == 0x0001)
        // ::/96 IPv4-compatible
        || (s[..6] == [0; 6] && is_private_network_ipv4(embedded_v4(s[6], s[7])))
        // 64:ff9b::/96 NAT64
        || (s[..6] == [0x0064, 0xff9b, 0, 0, 0, 0]
            && is_private_network_ipv4(embedded_v4(s[6], s[7])))
        // 2002::/16 6to4
        || (s[0] == 0x2002 && is_private_network_ipv4(embedded_v4(s[1], s[2])))
}

fn private_network_error(location: &str, target: &str) -> CubeError {
    CubeError::user(format!(
        "Import location '{}' points to a private network address ({}). Set {}=true to allow it.",
        location, target, ALLOW_PRIVATE_NETWORK_ENV
    ))
}

/// Rejects URLs whose host is a private IP literal or a `localhost` name. Hostnames are
/// resolved separately: by `check_resolved_host` up front and by `PublicOnlyResolver` on
/// every connection, since a name can resolve differently between the two.
pub fn check_url_host(location: &str, url: &Url) -> Result<(), CubeError> {
    match url.host() {
        Some(Host::Ipv4(ip)) if is_private_network_ipv4(ip) => {
            Err(private_network_error(location, &ip.to_string()))
        }
        Some(Host::Ipv6(ip)) if is_private_network_ipv6(ip) => {
            Err(private_network_error(location, &ip.to_string()))
        }
        Some(Host::Domain(domain)) if is_localhost_name(domain) => {
            Err(private_network_error(location, domain))
        }
        Some(_) => Ok(()),
        None => Err(CubeError::user(format!(
            "Import location '{}' has no host",
            location
        ))),
    }
}

fn is_localhost_name(domain: &str) -> bool {
    let domain = domain.trim_end_matches('.').to_ascii_lowercase();
    domain == "localhost" || domain.ends_with(".localhost")
}

/// Fails if `url`'s hostname resolves only to private addresses. Lookup errors are left
/// to the download itself, which retries them.
pub async fn check_resolved_host(location: &str, url: &Url) -> Result<(), CubeError> {
    let Some(Host::Domain(domain)) = url.host() else {
        return Ok(());
    };
    let Ok(addrs) = tokio::net::lookup_host((domain, 0)).await else {
        return Ok(());
    };
    let addrs = addrs.collect::<Vec<_>>();
    if !addrs.is_empty() && addrs.iter().all(|a| is_private_network_ip(a.ip())) {
        return Err(private_network_error(
            location,
            &format!("{} resolves to {}", domain, addrs[0].ip()),
        ));
    }
    Ok(())
}

/// Drops private addresses from every lookup, so a name that passed validation cannot be
/// re-pointed at the internal network (DNS rebinding) or reached through a redirect.
struct PublicOnlyResolver;

impl Resolve for PublicOnlyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let host = name.as_str().to_string();
            let addrs = tokio::net::lookup_host((host.as_str(), 0))
                .await?
                .collect::<Vec<SocketAddr>>();
            let public = addrs
                .iter()
                .copied()
                .filter(|a| !is_private_network_ip(a.ip()))
                .collect::<Vec<_>>();
            if public.is_empty() && !addrs.is_empty() {
                return Err(private_network_error(
                    &host,
                    &format!("{} resolves to {}", host, addrs[0].ip()),
                )
                .into());
            }
            Ok(Box::new(public.into_iter()) as Addrs)
        })
    }
}

/// HTTP client for `http(s)://` import locations.
pub fn http_location_client(allow_private_network: bool) -> Result<reqwest::Client, CubeError> {
    let builder = reqwest::Client::builder();
    let builder = if allow_private_network {
        builder
    } else {
        // A proxy would resolve the target host itself, out of reach of the resolver
        // filter, and an internal proxy host would be rejected by it.
        // IP literals bypass the resolver, so redirect targets are checked in the policy.
        builder
            .no_proxy()
            .dns_resolver(Arc::new(PublicOnlyResolver))
            .redirect(redirect::Policy::custom(|attempt| {
                if attempt.previous().len() >= MAX_REDIRECTS {
                    return attempt.error("too many redirects");
                }
                match check_url_host(attempt.url().as_str(), attempt.url()) {
                    Ok(()) => attempt.follow(),
                    Err(e) => attempt.error(e),
                }
            }))
    };
    Ok(builder.build()?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn private(ip: &str) -> bool {
        is_private_network_ip(ip.parse().unwrap())
    }

    #[test]
    fn private_ranges_are_detected() {
        for ip in [
            "127.0.0.1",
            "10.1.2.3",
            "172.16.0.1",
            "172.31.255.255",
            "192.168.1.1",
            "169.254.169.254",
            "100.100.100.200",
            "0.0.0.0",
            "0.1.2.3",
            "255.255.255.255",
            "198.18.0.1",
            "240.0.0.1",
            "224.0.0.1",
            "::",
            "::1",
            "fd00:ec2::254",
            "fe80::1",
            "::ffff:127.0.0.1",
            "::ffff:169.254.169.254",
            "::127.0.0.1",
            "64:ff9b::a00:1",
            "2002:7f00:1::",
        ] {
            assert!(private(ip), "{} must be private", ip);
        }
    }

    #[test]
    fn public_addresses_are_allowed() {
        for ip in [
            "8.8.8.8",
            "52.216.0.1",
            "172.32.0.1",
            "100.128.0.1",
            "2607:f8b0:4005:80a::200e",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2002:808:808::",
        ] {
            assert!(!private(ip), "{} must be public", ip);
        }
    }

    fn check(location: &str) -> Result<(), CubeError> {
        check_url_host(location, &Url::parse(location).unwrap())
    }

    #[test]
    fn url_hosts_are_checked() {
        for location in [
            "http://127.0.0.1:19099/data.csv",
            "http://[::1]/data.csv",
            "http://169.254.169.254/latest/meta-data/",
            "http://2130706433/data.csv",
            "http://0x7f.1/data.csv",
            "http://public.example.com@127.0.0.1/data.csv",
            "http://localhost:3030/data.csv",
            "http://LOCALHOST./data.csv",
            "http://api.localhost/data.csv",
        ] {
            let err = check(location).unwrap_err().message;
            assert!(
                err.contains(ALLOW_PRIVATE_NETWORK_ENV),
                "unexpected error for {}: {}",
                location,
                err
            );
        }
        assert!(check("https://bucket.s3.amazonaws.com/data.csv").is_ok());
        assert!(check("https://8.8.8.8/data.csv").is_ok());
    }
}
