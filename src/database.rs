use super::*;
use lru::LruCache;
use rusqlite::{params, Connection};
use rusqlite_migration::{Migrations, M};
use std::path::Path;

pub trait KvCache {
    fn get(&mut self, key: String) -> Result<Option<Vec<u8>>, CfKvFsError>;
    fn put(&mut self, key: String, value: Vec<u8>) -> Result<Vec<u8>, CfKvFsError>;
}

pub struct LruKvCache {
    cache: LruCache<String, Vec<u8>>,
}

impl LruKvCache {
    pub fn new() -> Box<dyn KvCache + Send + Sync> {
        Box::new(Self {
            cache: LruCache::new(128),
        })
    }
}

impl KvCache for LruKvCache {
    fn get(&mut self, key: String) -> Result<Option<Vec<u8>>, CfKvFsError> {
        Ok(self.cache.get(&key).map(|v| v.clone()))
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<Vec<u8>, CfKvFsError> {
        self.cache.put(key, value.clone());
        Ok(value)
    }
}

pub struct SqliteKvCache {
    cache: LruCache<i64, Vec<u8>>,
    conn: Arc<Mutex<Connection>>,
    name: String,
}

impl SqliteKvCache {
    pub fn new<P: AsRef<Path>, S: Into<String>>(
        path: P,
        name: S,
    ) -> Result<Box<dyn KvCache + Send + Sync>, CfKvFsError> {
        let name = name.into();
        let conn = Self::init_db(path, &name)?;

        Ok(Box::new(Self {
            cache: LruCache::new(128),
            conn: Arc::new(Mutex::new(conn)),
            name,
        }))
    }

    fn init_db<P: AsRef<Path>>(path: P, name: &str) -> Result<Connection, CfKvFsError> {
        let mut conn = Connection::open(path)?;
        Migrations::new(vec![M::up(&format!(
            "CREATE TABLE {}(key INTEGER NOT NULL, value BLOB NOT NULL);",
            &name
        ))])
        .to_latest(&mut conn)?;
        Ok(conn)
    }

    fn get_key_hash(key: String) -> i64 {
        get_hash(key)
    }
}

impl KvCache for SqliteKvCache {
    fn get(&mut self, key: String) -> Result<Option<Vec<u8>>, CfKvFsError> {
        let key = Self::get_key_hash(key);
        if let Some(value) = self.cache.get(&key) {
            Ok(Some(value.clone()))
        } else {
            let conn = self.conn.lock().unwrap();
            let mut stmt =
                conn.prepare(&format!("SELECT value FROM {} WHERE key = ?1", &self.name))?;
            let mut maps = stmt.query_map([key], |row| Ok(row.get(0)?))?;
            Ok(maps.next().and_then(|row| row.ok()))
        }
    }

    fn put(&mut self, key: String, value: Vec<u8>) -> Result<Vec<u8>, CfKvFsError> {
        if let Some(value) = self.get(key.clone())? {
            Ok(value)
        } else {
            let key = Self::get_key_hash(key);
            let conn = self.conn.lock().unwrap();
            let mut stmt = conn.prepare(&format!(
                "INSERT INTO {} (key, value) VALUES (?1, ?2)",
                &self.name
            ))?;
            if stmt.execute(params![key, value])? != 1 {
                Err(CfKvFsError::RusqliteError(
                    rusqlite::Error::ExecuteReturnedResults,
                ))
            } else {
                self.cache.put(key, value.clone());
                Ok(value)
            }
        }
    }
}
