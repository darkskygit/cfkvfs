mod database;
mod utils;

use database::{KvCache, LruKvCache, SqliteKvCache};
use lazy_static::lazy_static;
use log::error;
use rayon::prelude::*;
use reqwest::{
    blocking::Client,
    header::{HeaderMap, HeaderValue, AUTHORIZATION},
    redirect::Policy,
    Identity,
};
use std::{
    convert::TryInto,
    iter::FromIterator,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};
use thiserror::Error;
use utils::get_hash;

const CHUNK_SIZE: usize = 1024 * 1024;

#[derive(Debug, Error)]
pub enum CfKvFsError {
    #[error("Data transfer error: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("Database error: {0}")]
    RusqliteError(#[from] rusqlite::Error),
    #[error("Database migration error: {0}")]
    RusqliteMigrationError(#[from] rusqlite_migration::Error),
    #[error("Data parse error: {0}")]
    IntParseConvertError(#[from] std::array::TryFromSliceError),
    #[error("Data invalid")]
    HashError,
}

pub struct CfKvFsBuilder {
    endpoint: String,
    prefix: String,
    header: Option<HeaderMap>,
    pem: Option<Vec<u8>>,
    reducer: Option<Box<dyn Fn(Vec<u8>) -> Vec<u8> + Sync>>,
    path: Option<PathBuf>,
    table: Option<String>,
}

impl CfKvFsBuilder {
    fn new<E, P>(endpoint: E, prefix: P) -> Self
    where
        E: Into<String>,
        P: Into<String>,
    {
        Self {
            endpoint: endpoint.into(),
            prefix: prefix.into(),
            header: None,
            pem: None,
            reducer: None,
            path: None,
            table: None,
        }
    }

    pub fn auth(self, auth: &str) -> Self {
        if let Ok(auth) = HeaderValue::from_str(auth) {
            let header = HeaderMap::from_iter([(AUTHORIZATION, auth)]);
            self.header(header)
        } else {
            self
        }
    }

    pub fn header(mut self, header: HeaderMap) -> Self {
        self.header = Some(header);
        self
    }

    pub fn pem(mut self, pem: Vec<u8>) -> Self {
        self.pem = Some(pem);
        self
    }

    pub fn reducer<R: 'static + Fn(Vec<u8>) -> Vec<u8> + Sync>(mut self, reducer: R) -> Self {
        self.reducer = Some(Box::new(reducer));
        self
    }

    pub fn path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.path = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn table<S: Into<String>>(mut self, table: S) -> Self {
        self.table = Some(table.into());
        self
    }

    pub fn build(self) -> Option<CfKvFs> {
        CfKvFs::set_kv_cache(self.path, self.table);
        CfKvFs::inner_new(
            self.endpoint,
            self.prefix,
            self.header,
            self.pem,
            self.reducer,
        )
    }
}

pub struct CfKvFs {
    client: Client,
    endpoint: String,
    prefix: String,
    reducer: Option<Box<dyn Fn(Vec<u8>) -> Vec<u8> + Sync>>,
}

impl CfKvFs {
    pub fn builder<E, P>(endpoint: E, prefix: P) -> CfKvFsBuilder
    where
        E: Into<String>,
        P: Into<String>,
    {
        CfKvFsBuilder::new(endpoint, prefix)
    }

    pub fn new<E, P>(endpoint: E, prefix: P) -> Option<Self>
    where
        E: Into<String>,
        P: Into<String>,
    {
        Self::inner_new(endpoint, prefix, None, None, Some(Box::new(|data| data)))
    }

    fn inner_new<E, P>(
        endpoint: E,
        prefix: P,
        header: Option<HeaderMap>,
        pem: Option<Vec<u8>>,
        reducer: Option<Box<dyn Fn(Vec<u8>) -> Vec<u8> + Sync>>,
    ) -> Option<Self>
    where
        E: Into<String>,
        P: Into<String>,
    {
        let mut builder = Client::builder()
            .redirect(Policy::none())
            .no_proxy()
            .http2_prior_knowledge();
        if let Some(header) = header {
            builder = builder.default_headers(header);
        }
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
                reducer,
            })
        } else {
            None
        }
    }

    fn set_kv_cache(
        path: Option<PathBuf>,
        name: Option<String>,
    ) -> Arc<Mutex<Box<dyn KvCache + Send + Sync>>> {
        lazy_static! {
            static ref KV_PATH: Arc<RwLock<PathBuf>> = Arc::new(RwLock::new("./cache.db".into()));
            static ref KV_TABLE: Arc<RwLock<String>> = Arc::new(RwLock::new("kv".into()));
            static ref KV_CACHE: Arc<Mutex<Box<dyn KvCache + Send + Sync>>> = Arc::new(Mutex::new(
                SqliteKvCache::new(&*KV_PATH.read().unwrap(), &*KV_TABLE.read().unwrap())
                    .unwrap_or_else(|_| LruKvCache::new())
            ));
        }
        if let Some(path) = path {
            *KV_PATH.write().unwrap() = path;
        }
        if let Some(name) = name {
            *KV_TABLE.write().unwrap() = name;
        }
        KV_CACHE.clone()
    }

    fn get_kv_cache() -> Arc<Mutex<Box<dyn KvCache + Send + Sync>>> {
        Self::set_kv_cache(None, None)
    }

    fn post_data(&self, name: &str, data: Vec<u8>, index: bool) -> i64 {
        let mut retry = 0;
        let data = if let (Some(reducer), false) = (&self.reducer, index) {
            reducer(data)
        } else {
            data
        };
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
        let key = format!(
            "{}:{}",
            name,
            if hash == 0 {
                "index".into()
            } else {
                hash.to_string()
            }
        );
        if let Ok(Some(value)) = Self::get_kv_cache().lock().unwrap().get(key.clone()) {
            return Ok(value);
        }
        let mut retry = 0;
        let mut buf: Vec<u8> = vec![];
        while let Err(err) = self
            .client
            .get(format!("{}/{}/{}", self.endpoint, self.prefix, key))
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
        let data = Self::get_kv_cache().lock().unwrap().put(key, buf)?;
        if let (Some(reducer), false) = (&self.reducer, hash == 0) {
            Ok(reducer(data))
        } else {
            Ok(data)
        }
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
    let cf = CfKvFs::builder("https://darksky.eu.org", "fs")
        .reducer(|a| a.iter().chain(std::iter::once(&0)).cloned().collect())
        .pem(pem.to_vec())
        .build()
        .unwrap();
    cf.put_blob("test.bin", std::fs::read("test.bin").unwrap());
}

#[test]
fn test_download() {
    let cf = CfKvFs::builder("https://darksky.eu.org", "fs")
        .auth("Bearer 12345")
        .table("test1")
        .build()
        .unwrap();
    let bin = cf.get_blob("test.bin").unwrap();
    std::fs::write("test1.bin", bin).unwrap();
}
