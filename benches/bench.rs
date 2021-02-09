use std::sync::Arc;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::Throughput;
use fast_quic::Cert;
use fast_quic::Quic;
use futures::StreamExt;
use observability::tracing::*;
use once_cell::sync::OnceCell;
use tokio::runtime::{Builder, Runtime};
use url2::url2;
use url2::Url2;

const DATA: &[u8] = &[0xAB; 100];
const LARGE_DATA: &[u8] = &[0xAB; 1024 * 10];

const _NUM_RECV_CONCURRENT: usize = 100;

struct Client {
    quic: Quic,
    runtime: std::sync::Mutex<Runtime>,
}

#[allow(dead_code)]
pub struct Opt {
    /// How many client nodes should be spawned
    node_count: u32,

    /// Interval between requests per node
    request_interval_ms: u32,
}

// const PROCESS_DELAY_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

criterion_group!(benches, direct);

criterion_main!(benches);

fn passthrough(bench: &mut Criterion) {
    let _g = observability::test_run().ok();

    let proxy_url = spawn_proxy_quic();
    let _responder_url = spawn_responder_quic(proxy_url.clone());
    let (client, runtime) = make_client_quic(proxy_url);
    let client = Arc::new(client);
    let mut runtime = runtime.lock().unwrap();

    let mut group = bench.benchmark_group("passthrough");
    for &(data, messages, series) in [(DATA, 1000, true)].iter() {
        let bytes = ((data.len() * 2) as u64) * (messages as u64);
        group.throughput(Throughput::Bytes(bytes));
        group.sample_size(10);
        group.bench_with_input(
            BenchmarkId::new(
                "request",
                format!("messages_{}_series_{}_bytes_{}", messages, series, bytes,),
            ),
            &(data, messages),
            |b, &(data, messages)| {
                b.iter(|| {
                    let mut handles = Vec::new();

                    let client = client.clone();
                    if series {
                        handles.push(runtime.spawn(async move {
                            for _ in 0..messages {
                                let client = client.clone();
                                let (mut tx, mut rx) = client.open_bi().await.unwrap();
                                tx.write_all(data).await.unwrap();
                                let mut buf = Vec::with_capacity(1024 * 10);
                                tokio::io::AsyncReadExt::read_to_end(&mut rx, &mut buf)
                                    .await
                                    .unwrap();
                            }
                        }));
                    }

                    runtime.block_on(async {
                        for handle in handles {
                            handle.await.unwrap();
                        }
                    });
                })
            },
        );
    }
}

fn direct(bench: &mut Criterion) {
    let _g = observability::test_run().ok();

    let responder_url = spawn_responder_direct();
    let responder_url_bounce = spawn_responder_direct_bounce();
    let Client {
        quic,
        runtime: runtime_g,
    } = make_client();

    let mut runtime = runtime_g.lock().unwrap();
    let mut group = bench.benchmark_group("direct");
    for &data in [DATA, LARGE_DATA].iter() {
        let bytes = (data.len() * 2) as u64;
        group.throughput(Throughput::Bytes(bytes));
        group.sample_size(100);
        group.bench_with_input(
            BenchmarkId::new("baseline", format!("bytes_{}", bytes)),
            &data,
            |b, &_data| {
                b.iter(|| {
                    runtime.block_on(async {
                        let _buf: Vec<u8> = Vec::with_capacity(1024 * 10 * 2);
                    });
                })
            },
        );
        let con = runtime.block_on(async { quic.connect(responder_url.clone()).await });
        group.bench_with_input(
            BenchmarkId::new("new_stream", format!("bytes_{}", bytes)),
            &data,
            |b, &data| {
                b.iter(|| {
                    runtime.block_on(async {
                        let (mut tx, mut rx) = con.open_bi().await.unwrap();
                        tx.write_all(data).await.unwrap();
                        tx.finish().await.unwrap();
                        let mut buf = Vec::with_capacity(1024 * 10 * 2);
                        tokio::io::AsyncReadExt::read_to_end(&mut rx, &mut buf)
                            .await
                            .unwrap();
                    });
                })
            },
        );
        let mut buf = vec![0u8; data.len()];
        let (_con, (mut tx, mut rx)) = runtime.block_on(async {
            let con = quic.connect(responder_url_bounce.clone()).await;
            let stream = con.open_bi().await.unwrap();
            (con, stream)
        });
        group.bench_with_input(
            BenchmarkId::new("reuse_stream", format!("bytes_{}", bytes)),
            &data,
            |b, &data| {
                b.iter({
                    // let mut runtime = runtime_g.lock().unwrap();
                    || {
                        runtime.block_on(async {
                            buf = vec![0u8; data.len()];
                            tx.write_all(data).await.unwrap();
                            tokio::io::AsyncReadExt::read_exact(&mut rx, &mut buf)
                                .await
                                .unwrap();
                            assert_eq!(data[..], buf[..]);
                        });
                    }
                })
            },
        );
    }
}
fn spawn_responder_quic(proxy_url: Url2) -> Url2 {
    static INSTANCE: OnceCell<Url2> = OnceCell::new();
    INSTANCE
        .get_or_init(|| spawn_responder_inner(Some(proxy_url)))
        .clone()
}

fn spawn_responder_direct() -> Url2 {
    static INSTANCE: OnceCell<Url2> = OnceCell::new();
    INSTANCE.get_or_init(|| spawn_responder_inner(None)).clone()
}
fn spawn_responder_direct_bounce() -> Url2 {
    static INSTANCE: OnceCell<Url2> = OnceCell::new();
    INSTANCE
        .get_or_init(|| spawn_responder_inner_bounce(None))
        .clone()
}

fn spawn_responder_inner(_proxy_url: Option<Url2>) -> Url2 {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let _handle = std::thread::spawn(move || {
        let Client { quic, mut runtime } = make_client_inner();
        let address = quic.address().clone();
        info!("Responder Url: {}", address);
        tx.send(address).unwrap();
        runtime.get_mut().unwrap().block_on(async move {
            let mut incoming = quic.incoming();
            while let Some(con) = incoming.next().await {
                let quinn::NewConnection { mut bi_streams, .. } =
                    con.await.expect("connection failed");
                while let Some(Ok((mut send, mut recv))) = bi_streams.next().await {
                    let mut buf = Vec::with_capacity(1024 * 10 * 2);
                    tokio::io::AsyncReadExt::read_to_end(&mut recv, &mut buf)
                        .await
                        .unwrap();
                    send.write_all(&buf).await.unwrap();
                    send.finish().await.unwrap();
                }
            }
        });
        error!("Responder closed");
    });
    rx.recv().unwrap()
}

fn spawn_responder_inner_bounce(_proxy_url: Option<Url2>) -> Url2 {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let _handle = std::thread::spawn(move || {
        let Client { quic, mut runtime } = make_client_inner();
        let address = quic.address().clone();
        info!("Responder Url: {}", address);
        tx.send(address).unwrap();
        runtime.get_mut().unwrap().block_on(async move {
            let mut incoming = quic.incoming();
            while let Some(con) = incoming.next().await {
                let quinn::NewConnection { mut bi_streams, .. } =
                    con.await.expect("connection failed");
                while let Some(Ok((mut send, mut recv))) = bi_streams.next().await {
                    let mut buf = vec![0u8; 1024 * 10 * 2];
                    loop {
                        match tokio::io::AsyncReadExt::read(&mut recv, &mut buf).await {
                            Ok(read) => {
                                if read == 0 {
                                    continue;
                                }
                                if let Err(e) = send.write_all(&buf[..read]).await {
                                    error!(write_failed = ?e);
                                    break;
                                }
                            }
                            Err(e) => {
                                error!(read_failed = ?e);
                                break;
                            }
                        }
                    }
                    info!("Stream finished");
                }
            }
        });
        error!("Responder closed");
    });
    rx.recv().unwrap()
}

fn make_client() -> &'static Client {
    static INSTANCE: OnceCell<Client> = OnceCell::new();
    INSTANCE.get_or_init(|| make_client_inner())
}

fn make_client_quic(proxy_url: Url2) -> &'static (quinn::Connection, std::sync::Mutex<Runtime>) {
    static INSTANCE: OnceCell<(quinn::Connection, std::sync::Mutex<Runtime>)> = OnceCell::new();
    INSTANCE.get_or_init(|| {
        let Client { quic, mut runtime } = make_client_inner();
        let con = runtime
            .get_mut()
            .unwrap()
            .block_on(async move { quic.connect(proxy_url).await });
        (con, runtime)
    })
}

fn make_client_quic2(proxy_url: Url2) -> &'static (quinn::Connection, std::sync::Mutex<Runtime>) {
    static INSTANCE: OnceCell<(quinn::Connection, std::sync::Mutex<Runtime>)> = OnceCell::new();
    INSTANCE.get_or_init(|| {
        let Client { quic, mut runtime } = make_client_inner();
        let con = runtime
            .get_mut()
            .unwrap()
            .block_on(async move { quic.connect(proxy_url).await });
        (con, runtime)
    })
}

fn make_client_inner() -> Client {
    let mut runtime = rt();
    let client = runtime.block_on(async { Quic::new(cert()).await });
    Client {
        quic: client,
        runtime: std::sync::Mutex::new(runtime),
    }
}

fn spawn_proxy_quic() -> Url2 {
    static INSTANCE: OnceCell<Url2> = OnceCell::new();
    INSTANCE.get_or_init(|| spawn_proxy_inner()).clone()
}

fn spawn_proxy_inner() -> Url2 {
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let _handle = std::thread::spawn(move || {
        let mut runtime = rt();
        let handle = runtime.spawn(async move {
            let proxy_url = url2!("kitsune-quic://127.0.0.1:0");
            println!("Proxy Url: {}", proxy_url);
            tx.send(proxy_url).unwrap();

            error!("proxy CLOSED!");
        });
        let r = runtime.block_on(handle);
        error!("proxy CLOSED! {:?}", r);
    });
    rx.recv().unwrap()
}

fn rt() -> Runtime {
    Builder::new()
        .threaded_scheduler()
        .enable_all()
        .build()
        .unwrap()
}

fn cert() -> Cert {
    static INSTANCE: OnceCell<Cert> = OnceCell::new();
    let c = INSTANCE
        .get_or_init(|| {
            let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
            let key = quinn::PrivateKey::from_der(&cert.serialize_private_key_der()).unwrap();
            let cert = quinn::Certificate::from_der(&cert.serialize_der().unwrap()).unwrap();
            let chain = quinn::CertificateChain::from_certs(vec![cert.clone()]);
            Cert { cert, key, chain }
        })
        .clone();
    c
}
