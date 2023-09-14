use std::str::FromStr;
use std::sync::OnceLock;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use reqwest::header::{self, AsHeaderName, HeaderMap};
use tap::TapFallible;
use tracing::warn;

use crate::http::{Client, ClientResponse, ResponseHeaders};

impl ResponseHeaders for HeaderMap {
    fn header(&self, name: &str) -> Option<&str> {
        get_header_str(self, name)
    }
}

fn get_header_str<K: AsHeaderName>(headers: &HeaderMap, key: K) -> Option<&str> {
    headers.get(key).and_then(|val| {
        val.to_str()
            .tap_err(|e| warn!("error converting header value: {e:?}"))
            .ok()
    })
}

impl ClientResponse for reqwest::Response {
    type Error = reqwest::Error;
    type Headers = HeaderMap;

    fn content_length(&self) -> Option<u64> {
        get_header_str(self.headers(), header::CONTENT_LENGTH).and_then(|content_length| {
            u64::from_str(content_length)
                .tap_err(|e| warn!("invalid content length value: {e:?}"))
                .ok()
        })
    }

    fn content_type(&self) -> Option<&str> {
        get_header_str(self.headers(), header::CONTENT_TYPE)
    }

    fn headers(&self) -> Self::Headers {
        self.headers().clone()
    }

    fn is_success(&self) -> bool {
        self.status().is_success()
    }

    fn status_error(self) -> Result<(), Self::Error> {
        self.error_for_status().map(|_| ())
    }

    fn stream(self) -> Box<dyn Stream<Item = Result<Bytes, Self::Error>> + Unpin + Send + Sync> {
        Box::new(self.bytes_stream())
    }
}

// per reqwest's docs, it's advisable to create a single client and reuse it
static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

#[async_trait]
impl Client for reqwest::Client {
    type Url = reqwest::Url;
    type Response = reqwest::Response;
    type Error = reqwest::Error;
    type Headers = HeaderMap;

    fn create() -> Self {
        CLIENT.get_or_init(reqwest::Client::new).clone()
    }

    async fn get(&self, url: &Self::Url) -> Result<Self::Response, Self::Error> {
        let head = self.head(url.clone()).send().await.unwrap();
        // .content_length() returns Ok(0) for some reason? This doesn't tho
        let content_length = head.headers().get("content-length").unwrap().to_str().unwrap().parse::<u32>().unwrap();
        self.get(url.clone())
            .header(
                "Range",
                format!(
                    "bytes=0-{}",
                    // needed for youtube to disable rate limiting
                    // only works when content_length < 10MB
                    // (see here)[https://tyrrrz.me/blog/reverse-engineering-youtube-revisited]
                    // So I need to in theory implement multiple range requests
                    // But I'm too lazy rn
                    content_length,
                ),
            )
            .send().await
    }

    async fn get_range(
        &self,
        url: &Self::Url,
        start: u64,
        end: Option<u64>,
    ) -> Result<Self::Response, Self::Error> {
        self.get(url.clone())
            .header(
                "Range",
                format!(
                    "bytes={start}-{}",
                    end.map(|e| e.to_string()).unwrap_or_default()
                ),
            )
            .send()
            .await
    }
}
