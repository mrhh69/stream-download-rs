use std::{
    fs,
    io::{Read, Seek, SeekFrom},
    net::SocketAddr,
    pin::Pin,
    sync::OnceLock,
    task::{Context, Poll},
    time::Duration,
};

use crate::{http, Settings, StreamDownload};
use async_trait::async_trait;
use bytes::Bytes;
use ctor::ctor;
use futures::{Stream, StreamExt};
use rstest::rstest;
use tokio::{
    runtime::Runtime,
    sync::{mpsc, oneshot},
};
use tower_http::services::ServeDir;
use tracing_subscriber::EnvFilter;

struct TestClient {
    inner: reqwest::Client,
    tx: mpsc::Sender<(Command, oneshot::Sender<Duration>)>,
}

#[derive(Debug, PartialEq, Eq)]
enum Command {
    GetUrl,
    GetRange,
    ContentLength,
    NextChunk,
    EndStream,
}

struct TestResponse {
    inner: reqwest::Response,
    tx: mpsc::Sender<(Command, oneshot::Sender<Duration>)>,
}

enum StreamState {
    Ready,
    Waiting,
}

struct TestStream {
    inner: Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Unpin + Send + Sync>,
    tx: mpsc::Sender<(Command, oneshot::Sender<Duration>)>,
    state: StreamState,
}

impl Stream for TestStream {
    type Item = Result<Bytes, reqwest::Error>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.state {
            StreamState::Ready => {
                let (tx, rx) = oneshot::channel();
                self.tx.try_send((Command::NextChunk, tx)).unwrap();
                let waker = cx.waker().clone();
                self.state = StreamState::Waiting;

                tokio::spawn(async move {
                    tokio::time::sleep(rx.await.unwrap()).await;
                    waker.wake();
                });
                Poll::Pending
            }
            StreamState::Waiting => {
                self.state = StreamState::Ready;
                let mut this = Pin::new(self);
                let res = this.inner.poll_next_unpin(cx);

                match &res {
                    Poll::Ready(None) => {
                        let (tx, _rx) = oneshot::channel();
                        this.tx.try_send((Command::EndStream, tx)).unwrap();
                    }
                    Poll::Ready(Some(Ok(res))) if res.is_empty() => {
                        let (tx, _rx) = oneshot::channel();
                        this.tx.try_send((Command::EndStream, tx)).unwrap();
                    }
                    _ => {}
                };
                res
            }
        }
    }
}

impl TestClient {
    fn new(tx: mpsc::Sender<(Command, oneshot::Sender<Duration>)>) -> Self {
        Self {
            inner: reqwest::Client::new(),
            tx,
        }
    }
}

#[async_trait]
impl http::Client for TestClient {
    type Url = reqwest::Url;
    type Response = TestResponse;
    type Error = reqwest::Error;

    fn create() -> Self {
        unimplemented!()
    }

    async fn get(&self, url: &Self::Url) -> Result<Self::Response, Self::Error> {
        let (tx, rx) = oneshot::channel();
        self.tx.send((Command::GetUrl, tx)).await.unwrap();
        tokio::time::sleep(rx.await.unwrap()).await;

        http::Client::get(&self.inner, url)
            .await
            .map(|r| TestResponse {
                inner: r,
                tx: self.tx.clone(),
            })
    }

    async fn get_range(
        &self,
        url: &Self::Url,
        start: u64,
        end: Option<u64>,
    ) -> Result<Self::Response, Self::Error> {
        let (tx, rx) = oneshot::channel();
        self.tx.send((Command::GetRange, tx)).await.unwrap();
        tokio::time::sleep(rx.await.unwrap()).await;

        Ok(TestResponse {
            inner: self.inner.get_range(url, start, end).await?,
            tx: self.tx.clone(),
        })
    }
}

#[async_trait]
impl http::ClientResponse for TestResponse {
    type Error = reqwest::Error;

    async fn content_length(&self) -> Option<u64> {
        let (tx, rx) = oneshot::channel();
        self.tx.try_send((Command::ContentLength, tx)).unwrap();
        tokio::time::sleep(rx.await.unwrap()).await;
        http::ClientResponse::content_length(&self.inner).await
    }

    async fn is_success(&self) -> bool {
        self.inner.is_success().await
    }

    async fn status_error(self) -> String {
        self.inner.status_error().await
    }

    async fn stream(
        self,
    ) -> Box<dyn Stream<Item = Result<Bytes, Self::Error>> + Unpin + Send + Sync> {
        Box::new(TestStream {
            tx: self.tx.clone(),
            inner: self.inner.stream().await,
            state: StreamState::Ready,
        })
    }
}

static SERVER_RT: OnceLock<Runtime> = OnceLock::new();
static SERVER_ADDR: OnceLock<SocketAddr> = OnceLock::new();

#[ctor]
fn setup() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::default().add_directive("stream_download=trace".parse().unwrap()),
        )
        .with_line_number(true)
        .with_file(true)
        .with_test_writer()
        .init();

    let rt = SERVER_RT.get_or_init(|| Runtime::new().unwrap());
    let _guard = rt.enter();
    let service = ServeDir::new("./assets");

    let server = hyper::Server::try_bind(&"127.0.0.1:0".parse().unwrap())
        .unwrap()
        .serve(tower::make::Shared::new(service));
    SERVER_ADDR.get_or_init(|| server.local_addr());

    rt.spawn(async move {
        server.await.unwrap();
    });
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(256*1024)]
#[case(1024*1024)]
fn no_async(#[case] prefetch_bytes: u64) {
    let mut reader = StreamDownload::new_http(
        format!("http://{}/music.mp3", SERVER_ADDR.get().unwrap())
            .parse()
            .unwrap(),
        Settings { prefetch_bytes },
    )
    .unwrap();

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).unwrap();

    assert_eq!(get_file_buf(), buf);
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(256*1024)]
#[case(1024*1024)]
#[tokio::test(flavor = "multi_thread")]
async fn basic_download(#[case] prefetch_bytes: u64) {
    let mut reader = StreamDownload::new_http(
        format!("http://{}/music.mp3", SERVER_ADDR.get().unwrap())
            .parse()
            .unwrap(),
        Settings { prefetch_bytes },
    )
    .unwrap();

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).unwrap();

    assert_eq!(get_file_buf(), buf);
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(256*1024)]
#[case(1024*1024)]
#[tokio::test(flavor = "multi_thread")]
async fn slow_download(#[case] prefetch_bytes: u64) {
    let (tx, mut rx) = mpsc::channel(32);

    let mut reader = StreamDownload::from_make_stream(
        || {
            http::HttpStream::new(
                TestClient::new(tx),
                format!("http://{}/music.mp3", SERVER_ADDR.get().unwrap())
                    .parse()
                    .unwrap(),
            )
        },
        Settings { prefetch_bytes },
    )
    .unwrap();
    let handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (command, tx) = rx.recv().await.unwrap();
        assert_eq!(Command::GetUrl, command);
        tx.send(Duration::from_millis(50)).unwrap();

        let (command, tx) = rx.recv().await.unwrap();
        assert_eq!(Command::ContentLength, command);
        tx.send(Duration::from_millis(50)).unwrap();

        while let Some((command, tx)) = rx.recv().await {
            if command == Command::EndStream {
                return;
            }
            assert_eq!(Command::NextChunk, command);
            tx.send(Duration::from_millis(50)).unwrap();
        }
        panic!("Stream not finished");
    });

    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).unwrap();
    assert_eq!(get_file_buf(), buf);

    handle.await.unwrap();
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(256*1024)]
#[case(1024*1024)]
#[tokio::test(flavor = "multi_thread")]
async fn seek_basic(#[case] prefetch_bytes: u64) {
    let (tx, mut rx) = mpsc::channel(32);

    let mut reader = StreamDownload::from_make_stream(
        || {
            http::HttpStream::new(
                TestClient::new(tx),
                format!("http://{}/music.mp3", SERVER_ADDR.get().unwrap())
                    .parse()
                    .unwrap(),
            )
        },
        Settings { prefetch_bytes },
    )
    .unwrap();
    let handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (command, tx) = rx.recv().await.unwrap();
        assert_eq!(Command::GetUrl, command);
        tx.send(Duration::from_millis(50)).unwrap();

        let (command, tx) = rx.recv().await.unwrap();
        assert_eq!(Command::ContentLength, command);
        tx.send(Duration::from_millis(50)).unwrap();

        while let Some((command, tx)) = rx.recv().await {
            if command == Command::EndStream {
                return;
            }
            assert_eq!(Command::NextChunk, command);
            tx.send(Duration::from_millis(50)).unwrap();
        }
        panic!("Stream not finished");
    });

    let mut initial_buf = [0; 4096];
    reader.read_exact(&mut initial_buf).unwrap();
    reader.seek(SeekFrom::Start(0)).unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).unwrap();

    let file_buf = get_file_buf();
    assert_eq!(file_buf[0..4096], initial_buf);
    assert_eq!(file_buf, buf);

    handle.await.unwrap();
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(256*1024)]
#[case(1024*1024)]
#[tokio::test(flavor = "multi_thread")]
async fn seek_initial(#[case] prefetch_bytes: u64) {
    let (tx, mut rx) = mpsc::channel(32);

    let mut reader = StreamDownload::from_make_stream(
        || {
            http::HttpStream::new(
                TestClient::new(tx),
                format!("http://{}/music.mp3", SERVER_ADDR.get().unwrap())
                    .parse()
                    .unwrap(),
            )
        },
        Settings { prefetch_bytes },
    )
    .unwrap();

    let handle = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let (command, tx) = rx.recv().await.unwrap();
        assert_eq!(Command::GetUrl, command);
        tx.send(Duration::from_millis(50)).unwrap();

        let (command, tx) = rx.recv().await.unwrap();
        assert_eq!(Command::ContentLength, command);
        tx.send(Duration::from_millis(50)).unwrap();

        // Range request for first seek
        while let Some((command, tx)) = rx.recv().await {
            if command == Command::GetRange {
                tx.send(Duration::from_millis(50)).unwrap();
                break;
            }
            assert_eq!(Command::NextChunk, command);
            tx.send(Duration::from_millis(50)).unwrap();
        }

        // First download
        while let Some((command, tx)) = rx.recv().await {
            if command == Command::EndStream {
                break;
            }
            assert_eq!(Command::NextChunk, command);
            tx.send(Duration::from_millis(50)).unwrap();
        }

        // Range request for second seek
        while let Some((command, tx)) = rx.recv().await {
            if command == Command::GetRange {
                tx.send(Duration::from_millis(50)).unwrap();
                break;
            }
            assert_eq!(Command::NextChunk, command);
            tx.send(Duration::from_millis(50)).unwrap();
        }

        // Second download
        while let Some((command, tx)) = rx.recv().await {
            if command == Command::EndStream {
                return;
            }
            assert_eq!(Command::NextChunk, command);
            tx.send(Duration::from_millis(50)).unwrap();
        }
        panic!("stream not finished");
    });

    reader.seek(SeekFrom::Start(65536)).unwrap();

    let mut buf1 = Vec::new();
    reader.read_to_end(&mut buf1).unwrap();

    reader.seek(SeekFrom::Start(128)).unwrap();

    let mut buf2 = Vec::new();
    reader.read_to_end(&mut buf2).unwrap();

    let file_buf = get_file_buf();
    assert_eq!(file_buf[65536..], buf1);
    assert_eq!(file_buf[128..], buf2);

    handle.await.unwrap();
}

fn get_file_buf() -> Vec<u8> {
    fs::read("./assets/music.mp3").unwrap()
}
