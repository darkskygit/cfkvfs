mod utils;

use lazy_static::lazy_static;
use log::error;
use rayon::prelude::*;
use reqwest::{blocking::Client, redirect::Policy, Identity};
use std::convert::TryInto;
use thiserror::Error;
use utils::get_hash;

const CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Debug, Error)]
pub enum CfKvFsError {
    #[error("Data transfer error: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("Data parse error: {0}")]
    IntParseConvertError(#[from] std::array::TryFromSliceError),
    #[error("Data invalid")]
    HashError,
}

pub struct CfKvFs {
    client: Client,
    endpoint: String,
    prefix: String,
}

impl CfKvFs {
    pub fn new<E, P>(endpoint: E, prefix: P, pem: Option<Vec<u8>>) -> Option<Self>
    where
        E: Into<String>,
        P: Into<String>,
    {
        let mut builder = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .http2_prior_knowledge();
        if let Some(pem) = pem {
            if let Ok(identity) = Identity::from_pem(&pem) {
                builder = builder.identity(identity);
            }
        }
        if let Ok(client) = builder.build() {
            Some(Self {
                client,
                endpoint: endpoint.into(),
                prefix: prefix.into(),
            })
        } else {
            None
        }
    }

    fn post_data(&self, name: &str, data: Vec<u8>, index: bool) -> i64 {
        let mut retry = 0;
        let hash = get_hash(&data);
        while let Err(err) = self
            .client
            .post(format!(
                "{}/{}/{}:{}",
                self.endpoint,
                self.prefix,
                name,
                if index {
                    "index".into()
                } else {
                    hash.to_string()
                }
            ))
            .body(data.clone())
            .send()
        {
            if retry > 3 {
                error!("Failed to save blob: {}", err);
                return 0;
            } else {
                retry += 1;
            }
        }
        return hash;
    }

    pub fn put_blob(&self, name: &str, data: Vec<u8>) {
        let chunked_vec = data.chunks(CHUNK_SIZE).collect::<Vec<_>>();
        let hash_list = chunked_vec
            .par_iter()
            .map(|chunk| self.post_data(name, chunk.to_vec(), false).to_le_bytes())
            .flatten()
            .collect::<Vec<_>>();
        self.post_data(name, hash_list, true);
    }

    fn get_data(&self, name: &str, hash: i64) -> Result<Vec<u8>, CfKvFsError> {
        let mut retry = 0;
        let mut buf: Vec<u8> = vec![];
        while let Err(err) = self
            .client
            .get(format!(
                "{}/{}/{}:{}",
                self.endpoint,
                self.prefix,
                name,
                if hash == 0 {
                    "index".into()
                } else {
                    hash.to_string()
                }
            ))
            .send()
            .and_then(|mut resp| resp.copy_to(&mut buf))
            .map_err(CfKvFsError::ReqwestError)
            .and_then(|_| {
                if hash == 0 || get_hash(&buf) == hash {
                    Ok(())
                } else {
                    Err(CfKvFsError::HashError)
                }
            })
        {
            if retry > 3 {
                return Err(err);
            } else {
                retry += 1;
            }
        }
        Ok(buf)
    }

    pub fn get_blob(&self, name: &str) -> Result<Vec<u8>, CfKvFsError> {
        let data = self.get_data(name, 0)?;
        let hashes = data
            .chunks(8)
            .map(|hash| hash.try_into())
            .collect::<Result<Vec<[u8; 8]>, _>>()?;
        Ok(hashes
            .par_iter()
            .map(|hash| self.get_data(name, i64::from_le_bytes(*hash)))
            .collect::<Result<Vec<_>, CfKvFsError>>()?
            .iter()
            .flatten()
            .cloned()
            .collect())
    }
}

#[test]
fn test_upload() {
    let pem = include_bytes!("cert.pem");
    let cf = CfKvFs::new("https://darksky.eu.org", "fs/", Some(pem.to_vec())).unwrap();
    cf.put_blob("test.bin", std::fs::read("test.bin").unwrap());
}

#[test]
fn test_download() {
    let pem = include_bytes!("cert.pem");
    let cf = CfKvFs::new("https://darksky.eu.org", "fs/", Some(pem.to_vec())).unwrap();
    let bin = cf.get_blob("test.bin").unwrap();
    std::fs::write("test1.bin", bin).unwrap();
}
