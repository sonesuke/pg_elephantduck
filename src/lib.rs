use pgrx::pg_sys;
use pgrx::prelude::*;

pgrx::pg_module_magic!();

// Provide the metadata of function because pg_extern macro cannot handle raw pointer of TableAmRoutine.
#[no_mangle]
pub extern "C" fn pg_finfo_pg_elephantduck_handler() -> *const pg_sys::Pg_finfo_record {
    static V1: pg_sys::Pg_finfo_record = pg_sys::Pg_finfo_record { api_version: 1 };
    &V1
}

// The handler function for the access method.
// This function is called when the access method is created.
#[pg_guard]
#[no_mangle]
pub extern "C" fn pg_elephantduck_handler(_fcinfo: pg_sys::FunctionCallInfo) -> *mut pg_sys::TableAmRoutine {
    let table_am_routine = Box::new(pg_sys::TableAmRoutine {
        type_: pg_sys::NodeTag::T_TableAmRoutine,
        // example
        // insert: Some(my_insert_function),
        ..Default::default()
    });

    Box::into_raw(table_am_routine)
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
