use observability::tracing::*;
use quinn::ClientConfigBuilder;
use quinn::ServerConfigBuilder;
use url2::*;

pub struct Quic {
    endpoint: quinn::Endpoint,
    incoming: quinn::Incoming,
}

#[derive(Debug, Clone)]
pub struct Cert {
    pub cert: quinn::Certificate,
    pub key: quinn::PrivateKey,
    pub chain: quinn::CertificateChain,
}

impl Quic {
    pub async fn new(cert: Cert) -> Self {
        let bind_to = url2!("kitsune-quic://127.0.0.1:0");
        let mut client_config = ClientConfigBuilder::default();
        let server_config = quinn::ServerConfig::default();
        debug!(?server_config);
        let mut server_config = ServerConfigBuilder::new(server_config);
        let Cert {
            cert,
            key,
            chain: cert_chain,
        } = cert;
        server_config.certificate(cert_chain, key).unwrap();
        client_config.add_certificate_authority(cert).unwrap();
        let client_config = client_config.build();
        debug!(?client_config);

        let mut builder = quinn::Endpoint::builder();
        let server_config = server_config.build();
        debug!(?server_config);
        builder.listen(server_config);
        builder.default_client_config(client_config);

        let bind_to = format!(
            "{}:{}",
            bind_to.host_str().unwrap(),
            bind_to.port().unwrap()
        )
        .parse()
        .unwrap();
        let (endpoint, incoming) = builder.bind(&bind_to).unwrap();

        let s = Self { endpoint, incoming };
        info!("Listening on {}", s.address());
        s
    }

    pub async fn connect(&self, remote: Url2) -> quinn::Connection {
        info!("connecting to: {}", remote);
        let remote = format!("{}:{}", remote.host_str().unwrap(), remote.port().unwrap())
            .parse()
            .expect("Failed to parse remote address");
        let connection = self
            .endpoint
            .connect(&remote, "localhost")
            .unwrap()
            .await
            .unwrap();
        let quinn::NewConnection { connection, .. } = connection;
        connection
    }

    pub fn incoming(self) -> quinn::Incoming {
        self.incoming
    }

    pub fn address(&self) -> Url2 {
        let a = self
            .endpoint
            .local_addr()
            .expect("Failed to get local address")
            .to_string();
        Url2::parse(format!("kitsune-quic://{}", a))
    }
}
#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    fn cert() -> Cert {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let key = quinn::PrivateKey::from_der(&cert.serialize_private_key_der()).unwrap();
        let cert = quinn::Certificate::from_der(&cert.serialize_der().unwrap()).unwrap();
        let chain = quinn::CertificateChain::from_certs(vec![cert.clone()]);
        Cert { cert, key, chain }
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_new() {
        let _g = observability::test_run().ok();
        let cert_a = cert();
        let cert_b = cert_a.clone();
        let a = Quic::new(cert_a).await;
        let b = Quic::new(cert_b).await;
        let addr = b.address();
        let jh = tokio::spawn(async move {
            let mut incoming = b.incoming();
            let quinn::NewConnection { mut bi_streams, .. } =
                incoming.next().await.unwrap().await.unwrap();
            let (mut tx, rx) = bi_streams.next().await.unwrap().unwrap();
            let resp = rx.read_to_end(100).await.unwrap();
            tx.write_all(&resp).await.unwrap();
            tx.finish().await.unwrap();
        });
        let con = a.connect(addr).await;
        let (mut tx, rx) = con.open_bi().await.unwrap();
        tx.write_all(b"hey").await.unwrap();
        tx.finish().await.unwrap();
        let resp = rx.read_to_end(100).await.unwrap();
        assert_eq!(&resp, b"hey");
        jh.await.unwrap();
    }
}
