use pgrx::prelude::*;

mod tam;
mod tests;

pgrx::pg_module_magic!();

// Provide the metadata of function because pg_extern macro cannot handle raw pointer of TableAmRoutine.
#[no_mangle]
pub extern "C" fn pg_finfo_pg_elephantduck_handler() -> *const pg_sys::Pg_finfo_record {
    static V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1
}

// Register the extention as an access method.
extension_sql!(
    r#"
    CREATE FUNCTION pg_elephantduck_handler(internal) RETURNS table_am_handler
        AS 'MODULE_PATHNAME', 'pg_elephantduck_handler'
        LANGUAGE C STRICT;

    CREATE ACCESS METHOD elephantduck TYPE TABLE HANDLER pg_elephantduck_handler;
    "#,
    name = "create_elephantduck_access_method",
);

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
