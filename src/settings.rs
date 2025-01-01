use once_cell::sync::Lazy;
use std::env;
use std::ffi::{CStr, CString};
use std::fs;
use std::path::PathBuf;

use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

pub struct ElephantduckGucSettings {
    pub path: GucSetting<Option<&'static CStr>>,
    pub threads: GucSetting<i32>,
}

impl ElephantduckGucSettings {
    pub fn new() -> Self {
        let default_path = {
            let data_root_cstring = match env::var("PGDATA") {
                Ok(pgdata) => pgdata,
                Err(_) => "/tmp".to_string(),
            };

            let mut path = PathBuf::from(data_root_cstring);
            path.push("elephantduck");

            // if the directory does not exist, create it
            if !path.exists() {
                fs::create_dir_all(&path).expect("Failed to create directory");
            }

            let path_str = path.to_str().expect("Failed to convert path to str");
            let c_string = CString::new(path_str).expect("CString::new failed");
            Box::leak(c_string.into_boxed_c_str())
        };

        Self {
            path: GucSetting::<Option<&'static CStr>>::new(Some(default_path)),
            threads: GucSetting::<i32>::new(4),
        }
    }

    pub fn init(&self) {
        GucRegistry::define_string_guc(
            "elephantduck.path",
            "Specifies the directory where ElephantDuck will store parquet files.",
            "Specifies the directory where ElephantDuck will store parquet files.",
            &self.path,
            GucContext::Userset,
            GucFlags::default(),
        );

        GucRegistry::define_int_guc(
            "elephantduck.threads",
            "Specifies the number of threads to use for ElephantDuck operations.",
            "Specifies the number of threads to use for ElephantDuck operations.",
            &self.threads,
            1,
            64,
            GucContext::Userset,
            GucFlags::default(),
        );
    }
}

impl Default for ElephantduckGucSettings {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(clippy::redundant_closure)]
pub static ELEPHANTDUCK_GUCS: Lazy<ElephantduckGucSettings> = Lazy::new(|| ElephantduckGucSettings::new());

pub fn init_gucs() {
    ELEPHANTDUCK_GUCS.init();
}

pub fn get_elephantduck_path() -> Option<&'static CStr> {
    ELEPHANTDUCK_GUCS.path.get()
}

pub fn get_elephantduck_threads() -> i32 {
    ELEPHANTDUCK_GUCS.threads.get()
}
