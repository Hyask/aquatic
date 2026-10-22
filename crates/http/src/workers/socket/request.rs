use std::net::IpAddr;

use anyhow::Context;
use aquatic_http_protocol::request::Request;

use crate::config::{Config, ReverseProxyPeerIpHeaderFormat};

#[derive(Debug, thiserror::Error)]
pub enum RequestParseError {
    #[error("required peer ip header missing or invalid")]
    RequiredPeerIpHeaderMissing(anyhow::Error),
    #[error("more data needed")]
    MoreDataNeeded,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Header values from a request that are needed to build the announce URL
/// advertised on the index page.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestMetadata {
    /// Value of the `Host` header, if present
    pub opt_host: Option<String>,
    /// Value of the `X-Forwarded-Proto` header, if present
    pub opt_forwarded_proto: Option<String>,
}

pub fn parse_request(
    config: &Config,
    buffer: &[u8],
) -> Result<(Request, Option<IpAddr>, RequestMetadata), RequestParseError> {
    let mut headers = [httparse::EMPTY_HEADER; 16];
    let mut http_request = httparse::Request::new(&mut headers);

    match http_request.parse(buffer).with_context(|| "httparse")? {
        httparse::Status::Complete(_) => {
            let path = http_request.path.ok_or(anyhow::anyhow!("no http path"))?;
            let request = Request::parse_http_get_path(path)?;

            if matches!(request, Request::Index) && !config.run_index {
                return Err(RequestParseError::Other(anyhow::anyhow!(
                    "index page is not enabled"
                )));
            }

            let opt_peer_ip = if config.network.runs_behind_reverse_proxy {
                let header_name = &config.network.reverse_proxy_ip_header_name;
                let header_format = config.network.reverse_proxy_ip_header_format;

                match parse_forwarded_header(header_name, header_format, http_request.headers) {
                    Ok(peer_ip) => Some(peer_ip),
                    Err(err) => {
                        return Err(RequestParseError::RequiredPeerIpHeaderMissing(err));
                    }
                }
            } else {
                None
            };

            let metadata = RequestMetadata {
                opt_host: parse_header_value("host", http_request.headers),
                opt_forwarded_proto: parse_header_value("x-forwarded-proto", http_request.headers),
            };

            Ok((request, opt_peer_ip, metadata))
        }
        httparse::Status::Partial => Err(RequestParseError::MoreDataNeeded),
    }
}

/// Return the value of the first case-insensitively matching header, if any.
fn parse_header_value(header_name: &str, headers: &[httparse::Header<'_>]) -> Option<String> {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case(header_name))
        .and_then(|header| std::str::from_utf8(header.value).ok())
        .map(|value| value.trim().to_string())
}

fn parse_forwarded_header(
    header_name: &str,
    header_format: ReverseProxyPeerIpHeaderFormat,
    headers: &[httparse::Header<'_>],
) -> anyhow::Result<IpAddr> {
    for header in headers.iter().rev() {
        if header.name == header_name {
            match header_format {
                ReverseProxyPeerIpHeaderFormat::LastAddress => {
                    return ::std::str::from_utf8(header.value)?
                        .split(',')
                        .last()
                        .ok_or(anyhow::anyhow!("no header value"))?
                        .trim()
                        .parse::<IpAddr>()
                        .with_context(|| "parse ip");
                }
            }
        }
    }

    Err(anyhow::anyhow!("header not present"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const REQUEST_START: &str = "GET /announce?info_hash=%04%0bkV%3f%5cr%14%a6%b7%98%adC%c3%c9.%40%24%00%b9&peer_id=-ABC940-5ert69muw5t8&port=12345&uploaded=1&downloaded=2&left=3&numwant=0&key=4ab4b877&compact=1&supportcrypto=1&event=started HTTP/1.1\r\nHost: example.com\r\n";

    #[test]
    fn test_parse_enabled_index_request() {
        let mut config = Config::default();

        config.run_index = true;

        let request = b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n";

        assert_eq!(parse_request(&config, request).unwrap().0, Request::Index);
    }

    #[test]
    fn test_parse_request_metadata() {
        let mut config = Config::default();

        config.run_index = true;

        let request =
            b"GET / HTTP/1.1\r\nHost: example.com:3000\r\nX-Forwarded-Proto: https\r\n\r\n";

        let metadata = parse_request(&config, request).unwrap().2;

        assert_eq!(metadata.opt_host.as_deref(), Some("example.com:3000"));
        assert_eq!(metadata.opt_forwarded_proto.as_deref(), Some("https"));
    }

    #[test]
    fn test_parse_disabled_index_request() {
        let config = Config::default();

        let request = b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n";

        assert!(matches!(
            parse_request(&config, request),
            Err(RequestParseError::Other(_))
        ));
    }

    #[test]
    fn test_parse_peer_ip_header_multiple() {
        let mut config = Config::default();

        config.network.runs_behind_reverse_proxy = true;
        config.network.reverse_proxy_ip_header_name = "X-Forwarded-For".into();
        config.network.reverse_proxy_ip_header_format = ReverseProxyPeerIpHeaderFormat::LastAddress;

        let mut request = REQUEST_START.to_string();

        request.push_str("X-Forwarded-For: 200.0.0.1\r\n");
        request.push_str("X-Forwarded-For: 1.2.3.4, 5.6.7.8,9.10.11.12\r\n");
        request.push_str("\r\n");

        let expected_ip = IpAddr::from([9, 10, 11, 12]);

        assert_eq!(
            parse_request(&config, request.as_bytes())
                .unwrap()
                .1
                .unwrap(),
            expected_ip
        )
    }

    #[test]
    fn test_parse_peer_ip_header_single() {
        let mut config = Config::default();

        config.network.runs_behind_reverse_proxy = true;
        config.network.reverse_proxy_ip_header_name = "X-Forwarded-For".into();
        config.network.reverse_proxy_ip_header_format = ReverseProxyPeerIpHeaderFormat::LastAddress;

        let mut request = REQUEST_START.to_string();

        request.push_str("X-Forwarded-For: 1.2.3.4, 5.6.7.8,9.10.11.12\r\n");
        request.push_str("X-Forwarded-For: 200.0.0.1\r\n");
        request.push_str("\r\n");

        let expected_ip = IpAddr::from([200, 0, 0, 1]);

        assert_eq!(
            parse_request(&config, request.as_bytes())
                .unwrap()
                .1
                .unwrap(),
            expected_ip
        )
    }

    #[test]
    fn test_parse_peer_ip_header_no_header() {
        let mut config = Config::default();

        config.network.runs_behind_reverse_proxy = true;

        let mut request = REQUEST_START.to_string();

        request.push_str("\r\n");

        let res = parse_request(&config, request.as_bytes());

        assert!(matches!(
            res,
            Err(RequestParseError::RequiredPeerIpHeaderMissing(_))
        ));
    }
}
