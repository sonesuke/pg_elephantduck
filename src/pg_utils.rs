use pgrx::pg_sys::*;
use pgrx::*;

use crate::tam::ELEPHANTDUCK_AM_ROUTINE;

pub trait IsElephantDuckTable {
    fn open_from_name_list(names: *mut List) -> std::result::Result<PgRelation, &'static str>;
    fn is_elephantduck_table(&self) -> bool;
}

impl IsElephantDuckTable for PgRelation {
    fn open_from_name_list(names: *mut List) -> std::result::Result<PgRelation, &'static str> {
        unsafe {
            let rel = makeRangeVarFromNameList(names);
            let relid = RangeVarGetRelidExtended(
                rel,
                AccessShareLock as i32,
                RVROption::RVR_MISSING_OK,
                None,
                std::ptr::null_mut(),
            );
            match relid {
                #[allow(non_upper_case_globals)]
                InvalidOid => Err("relation not found"),
                _ => Ok(PgRelation::open(relid)),
            }
        }
    }
    fn is_elephantduck_table(&self) -> bool {
        if self.is_null() {
            return false;
        }
        unsafe { self.as_ref().rd_tableam == ELEPHANTDUCK_AM_ROUTINE.lock().unwrap().get_routines() }
    }
}

pub fn search_namelist(list: *mut List) -> *mut List {
    let candidates = unsafe { PgList::<pg_sys::Node>::from_pg(list) };
    if candidates.is_empty() {
        return std::ptr::null_mut();
    }

    let head = candidates.head().unwrap();
    unsafe {
        match head {
            node if is_a(node, NodeTag::T_String) => list,
            node if is_a(node, NodeTag::T_List) => search_namelist(node as *mut List),
            _ => std::ptr::null_mut(),
        }
    }
}
