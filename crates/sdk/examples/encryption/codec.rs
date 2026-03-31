#![allow(unreachable_pub)]
use futures::FutureExt;
use futures::future::BoxFuture;
use std::collections::HashMap;
use temporalio_common::data_converters::{PayloadCodec, SerializationContextData};
use temporalio_common::protos::temporal::api::common::v1::Payload;

const ENCODING_KEY: &str = "encoding";
const ENCRYPTED_ENCODING: &[u8] = b"binary/encrypted";

/// A simple XOR-based PayloadCodec that demonstrates the encryption pattern.
///
/// In production, use a real encryption library (e.g., AES-GCM via `ring` or `aes-gcm`).
pub struct EncryptionCodec {
    pub key: Vec<u8>,
}

impl EncryptionCodec {
    fn xor_bytes(&self, data: &[u8]) -> Vec<u8> {
        data.iter()
            .enumerate()
            .map(|(i, b)| b ^ self.key[i % self.key.len()])
            .collect()
    }
}

impl PayloadCodec for EncryptionCodec {
    fn encode(
        &self,
        _context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        let encoded: Vec<Payload> = payloads
            .into_iter()
            .map(|p| {
                let serialized = p.data;
                let encrypted = self.xor_bytes(&serialized);
                let mut metadata = HashMap::new();
                metadata.insert(ENCODING_KEY.to_string(), ENCRYPTED_ENCODING.to_vec());
                Payload {
                    metadata,
                    data: encrypted,
                    ..Default::default()
                }
            })
            .collect();
        async move { encoded }.boxed()
    }

    fn decode(
        &self,
        _context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        let decoded: Vec<Payload> = payloads
            .into_iter()
            .map(|p| {
                let is_encrypted = p
                    .metadata
                    .get(ENCODING_KEY)
                    .map(|v| v.as_slice() == ENCRYPTED_ENCODING)
                    .unwrap_or(false);

                if is_encrypted {
                    let decrypted = self.xor_bytes(&p.data);
                    Payload {
                        data: decrypted,
                        metadata: HashMap::new(),
                        ..Default::default()
                    }
                } else {
                    p
                }
            })
            .collect();
        async move { decoded }.boxed()
    }
}
