use super::*;
use sha3::{
    digest::{ExtendableOutputDirty, Reset, Update},
    Shake256,
};
use std::{
    convert::TryInto,
    io::Read,
    sync::{Arc, Mutex},
};

pub fn get_hash<D: AsRef<[u8]>>(data: D) -> i64 {
    lazy_static! {
        static ref HASHER: Arc<Mutex<Shake256>> = Arc::new(Mutex::new(Shake256::default()));
    }
    i64::from_ne_bytes(
        {
            let mut hasher = HASHER.lock().unwrap();
            hasher.update(data);
            let mut ret = hasher.clone();
            hasher.reset();
            let mut buf = vec![0u8; 8];
            if ret.finalize_xof_dirty().read(&mut buf).is_err() {
                buf = vec![0u8, 8];
            }
            buf
        }
        .as_slice()
        .try_into()
        .expect("slice with incorrect length"),
    )
}
