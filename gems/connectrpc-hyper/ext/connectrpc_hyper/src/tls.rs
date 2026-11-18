//! rustls client configuration: aws-lc-rs with post-quantum key exchange
//! preferred, TLS 1.2 minimum, no session resumption, native roots (loaded
//! once per process, honouring SSL_CERT_FILE/SSL_CERT_DIR at first use) plus
//! the transport's appended `ca_file` certificates. Ruby validates inputs with
//! OpenSSL and passes DER.
use std::sync::Arc;

use rustls::client::Resumption;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::sign::CertifiedKey;
use rustls::{ClientConfig, RootCertStore};

use crate::failure::Failure;
use crate::runtime::PidCell;

static NATIVE_ROOTS: PidCell<Arc<RootCertStore>> = PidCell::new();

fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// The platform trust store, or SSL_CERT_FILE/SSL_CERT_DIR when set, loaded
/// once per process. Blocking file I/O: call from a blocking context.
pub fn native_roots() -> Arc<RootCertStore> {
    NATIVE_ROOTS
        .get_or_init(|| {
            let mut roots = RootCertStore::empty();
            roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
            Arc::new(roots)
        })
        .clone()
}

/// Validated TLS inputs; construction fails with the name of the input.
pub struct TlsInputs {
    extra_roots: Vec<CertificateDer<'static>>,
    identity: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
}

impl TlsInputs {
    pub fn new(
        roots: Vec<Vec<u8>>,
        chain: Option<Vec<Vec<u8>>>,
        key: Option<Vec<u8>>,
    ) -> Result<Self, &'static str> {
        let extra_roots: Vec<CertificateDer<'static>> =
            roots.into_iter().map(CertificateDer::from).collect();
        // Every appended certificate must become a trust anchor.
        let mut check = RootCertStore::empty();
        for root in &extra_roots {
            check.add(root.clone()).map_err(|_| "ca_file")?;
        }
        let identity = match (chain, key) {
            (Some(chain), Some(key)) => {
                let chain: Vec<CertificateDer<'static>> =
                    chain.into_iter().map(CertificateDer::from).collect();
                let provider = provider();
                // Imports the key into the provider: a key OpenSSL accepts but
                // aws-lc-rs cannot sign with (Ed448) fails here.
                let key: PrivateKeyDer<'static> = PrivatePkcs8KeyDer::from(key).into();
                let signing = provider
                    .key_provider
                    .load_private_key(key.clone_key())
                    .map_err(|_| "private_key")?;
                let certified = CertifiedKey::new(chain.clone(), signing);
                match certified.keys_match() {
                    Ok(()) => {}
                    Err(rustls::Error::InconsistentKeys(rustls::InconsistentKeys::Unknown)) => {}
                    Err(rustls::Error::InconsistentKeys(_)) => {
                        return Err("private_key");
                    }
                    Err(_) => return Err("certificate_chain"),
                }
                Some((chain, key))
            }
            (None, None) => None,
            (Some(_), None) => return Err("private_key"),
            (None, Some(_)) => return Err("certificate_chain"),
        };
        Ok(Self {
            extra_roots,
            identity,
        })
    }

    /// Builds the h2 (ALPN h2, http/1.1) and HTTP/1.1-only configurations.
    pub fn configs(
        &self,
        native: &RootCertStore,
    ) -> Result<(Arc<ClientConfig>, Arc<ClientConfig>), Failure> {
        let mut roots = native.clone();
        for root in &self.extra_roots {
            roots
                .add(root.clone())
                .map_err(|_| Failure::new("tls", "invalid trust anchor"))?;
        }
        let builder = ClientConfig::builder_with_provider(provider())
            .with_protocol_versions(&[&rustls::version::TLS13, &rustls::version::TLS12])
            .map_err(|_| Failure::new("tls", "unsupported protocol versions"))?
            .with_root_certificates(roots);
        let mut config = match &self.identity {
            Some((chain, key)) => builder
                .with_client_auth_cert(chain.clone(), key.clone_key())
                .map_err(|_| Failure::new("tls", "client identity rejected"))?,
            None => builder.with_no_client_auth(),
        };
        config.resumption = Resumption::disabled();
        let mut h1 = config.clone();
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        h1.alpn_protocols = vec![b"http/1.1".to_vec()];
        Ok((Arc::new(config), Arc::new(h1)))
    }
}
