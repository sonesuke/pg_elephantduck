// table_ access method (TM) interface

use pgrx::pg_sys::*;

#[allow(unused_imports)]
use pgrx::prelude::*;

use once_cell::sync::Lazy;
use std::sync::Mutex;

use crate::storage::*;

struct PgElephantduckAmRoutine {
    routines: TableAmRoutine,
}

impl PgElephantduckAmRoutine {
    fn new() -> Self {
        PgElephantduckAmRoutine {
            routines: TableAmRoutine {
                type_: NodeTag::T_TableAmRoutine,

                slot_callbacks: Some(pg_elephantduck_slot_callbacks),
                scan_begin: Some(pg_elephantduck_scan_begin),
                scan_end: Some(pg_elephantduck_scan_end),
                scan_rescan: Some(pg_elephantduck_scan_rescan),
                scan_getnextslot: Some(pg_elephantduck_scan_getnextslot),
                scan_set_tidrange: Some(pg_elephantduck_scan_set_tidrange),
                scan_getnextslot_tidrange: Some(pg_elephantduck_scan_getnextslot_tidrange),
                parallelscan_estimate: Some(pg_elephantduck_parallelscan_estimate),
                parallelscan_initialize: Some(pg_elephantduck_parallelscan_initialize),
                parallelscan_reinitialize: Some(pg_elephantduck_parallelscan_reinitialize),
                index_fetch_begin: Some(pg_elephantduck_index_fetch_begin),
                index_fetch_reset: Some(pg_elephantduck_index_fetch_reset),
                index_fetch_end: Some(pg_elephantduck_index_fetch_end),
                index_fetch_tuple: Some(pg_elephantduck_index_fetch_tuple),
                tuple_fetch_row_version: Some(pg_elephantduck_tuple_fetch_row_version),
                tuple_tid_valid: Some(pg_elephantduck_tuple_tid_valid),
                tuple_get_latest_tid: Some(pg_elephantduck_tuple_get_latest_tid),
                tuple_satisfies_snapshot: Some(pg_elephantduck_tuple_satisfies_snapshot),
                index_delete_tuples: Some(pg_elephantduck_index_delete_tuples),
                tuple_insert: Some(pg_elephantduck_tuple_insert),
                tuple_insert_speculative: Some(pg_elephantduck_tuple_insert_speculative),
                tuple_complete_speculative: Some(pg_elephantduck_tuple_complete_speculative),
                multi_insert: Some(pg_elephantduck_multi_insert),
                tuple_delete: Some(pg_elephantduck_tuple_delete),
                tuple_update: Some(pg_elephantduck_tuple_update),
                tuple_lock: Some(pg_elephantduck_tuple_lock),
                finish_bulk_insert: Some(pg_elephantduck_finish_bulk_insert),
                relation_set_new_filelocator: Some(pg_elephantduck_relation_set_new_filelocator),
                relation_nontransactional_truncate: Some(pg_elephantduck_relation_nontransactional_truncate),
                relation_copy_data: Some(pg_elephantduck_relation_copy_data),
                relation_copy_for_cluster: Some(pg_elephantduck_relation_copy_for_cluster),
                relation_vacuum: Some(pg_elephantduck_relation_vacuum),
                scan_analyze_next_block: Some(pg_elephantduck_scan_analyze_next_block),
                scan_analyze_next_tuple: Some(pg_elephantduck_scan_analyze_next_tuple),
                index_build_range_scan: Some(pg_elephantduck_index_build_range_scan),
                index_validate_scan: Some(pg_elephantduck_index_validate_scan),
                relation_size: Some(pg_elephantduck_relation_size),
                relation_needs_toast_table: Some(pg_elephantduck_relation_needs_toast_table),
                relation_toast_am: Some(pg_elephantduck_relation_toast_am),
                relation_fetch_toast_slice: Some(pg_elephantduck_relation_fetch_toast_slice),
                relation_estimate_size: Some(pg_elephantduck_relation_estimate_size),
                scan_bitmap_next_block: Some(pg_elephantduck_scan_bitmap_next_block),
                scan_bitmap_next_tuple: Some(pg_elephantduck_scan_bitmap_next_tuple),
                scan_sample_next_block: Some(pg_elephantduck_scan_sample_next_block),
                scan_sample_next_tuple: Some(pg_elephantduck_scan_sample_next_tuple),
            },
        }
    }

    fn get_routines(&self) -> *mut TableAmRoutine {
        &self.routines as *const _ as *mut _
    }
}

static mut ELEPHANTDUCK_AM_ROUTINE: Lazy<Mutex<PgElephantduckAmRoutine>> =
    Lazy::new(|| Mutex::new(PgElephantduckAmRoutine::new()));

fn get_schema_from_relation(rel: Relation) -> Box<Schema> {
    unsafe {
        let tuple_desc = (*rel).rd_att;
        let natts = (*tuple_desc).natts as usize;
        let attrs = (*tuple_desc).attrs.as_slice(natts);
        Box::new(Schema {
            fields: attrs
                .iter()
                .filter(|attr| !attr.is_dropped())
                .map(|a| Attribute {
                    column_id: a.attnum,
                    data_type: a.atttypid,
                })
                .collect(),
            where_clause: None,
            sample_clause: None,
        })
    }
}

// The handler function for the access method.
// This function is called when the access method is created.
#[pg_guard]
#[no_mangle]
pub extern "C" fn pg_elephantduck_handler(_fcinfo: FunctionCallInfo) -> *mut TableAmRoutine {
    unsafe { ELEPHANTDUCK_AM_ROUTINE.lock().unwrap().get_routines() }
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_slot_callbacks(_rel: Relation) -> *const TupleTableSlotOps {
    // Minimal Implement.
    // See https://github.com/postgres/postgres/blob/master/src/include/executor/tuptable.h#L33
    &TTSOpsVirtual
}

#[allow(dead_code)]
pub struct ElephantDuckScan {
    rs_base: TableScanDescData, // Base class from access/relscan.h.
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_begin(
    rel: Relation,
    snapshot: Snapshot,
    nkeys: std::ffi::c_int,
    key: *mut ScanKeyData,
    pscan: ParallelTableScanDesc,
    flags: uint32,
) -> TableScanDesc {
    set_schema_for_read((*rel).rd_id.into(), *get_schema_from_relation(rel));
    let scan = Box::new(ElephantDuckScan {
        rs_base: TableScanDescData {
            rs_rd: rel,
            rs_snapshot: snapshot,
            rs_nkeys: nkeys,
            rs_key: key,
            rs_flags: flags,
            rs_parallel: pscan,
            rs_maxtid: ItemPointerData { ..Default::default() },
            rs_mintid: ItemPointerData { ..Default::default() },
        },
    });
    Box::into_raw(scan) as TableScanDesc
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_end(scan: TableScanDesc) {
    if !scan.is_null() {
        let _ = Box::from_raw(scan as *mut ElephantDuckScan);
    }
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_rescan(
    _scan: TableScanDesc,
    _key: *mut ScanKeyData,
    _set_params: bool,
    _allow_strat: bool,
    _allow_sync: bool,
    _allow_pagemode: bool,
) {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_getnextslot(
    scan: TableScanDesc,
    _direction: ScanDirection::Type,
    slot: *mut TupleTableSlot,
) -> bool {
    ExecClearTuple(slot);
    let elephantduck_scan = scan as *mut ElephantDuckScan;
    let relid = (*(*elephantduck_scan).rs_base.rs_rd).rd_id;

    let tuple_descriptor = (*slot).tts_tupleDescriptor;
    let natts: usize = (*tuple_descriptor).natts as usize;
    let mut row = TupleSlot {
        natts,
        datum: std::slice::from_raw_parts_mut((*slot).tts_values, natts),
        nulls: std::slice::from_raw_parts_mut((*slot).tts_isnull, natts),
    };

    if read(relid.into(), &mut row) {
        ExecStoreVirtualTuple(slot);
        true
    } else {
        false
    }
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_set_tidrange(
    _scan: TableScanDesc,
    _mintid: ItemPointer,
    _maxtid: ItemPointer,
) {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_getnextslot_tidrange(
    _scan: TableScanDesc,
    _direction: ScanDirection::Type,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_parallelscan_estimate(_rel: Relation) -> Size {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_parallelscan_initialize(_rel: Relation, _pscan: ParallelTableScanDesc) -> Size {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_parallelscan_reinitialize(_rel: Relation, _pscan: ParallelTableScanDesc) {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_index_fetch_begin(_rel: Relation) -> *mut IndexFetchTableData {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_index_fetch_reset(_data: *mut IndexFetchTableData) {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_index_fetch_end(_data: *mut IndexFetchTableData) {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_index_fetch_tuple(
    _scan: *mut IndexFetchTableData,
    _tid: ItemPointer,
    _snapshot: Snapshot,
    _slot: *mut TupleTableSlot,
    _call_again: *mut bool,
    _all_dead: *mut bool,
) -> bool {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_fetch_row_version(
    _rel: Relation,
    _tid: ItemPointer,
    _snapshot: Snapshot,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_tid_valid(_scan: TableScanDesc, _tid: ItemPointer) -> bool {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_get_latest_tid(_scan: TableScanDesc, _tid: ItemPointer) {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_satisfies_snapshot(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _snapshot: Snapshot,
) -> bool {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_index_delete_tuples(
    _rel: Relation,
    _delstate: *mut TM_IndexDeleteOp,
) -> TransactionId {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_insert(
    rel: Relation,
    slot: *mut TupleTableSlot,
    _cid: CommandId,
    _options: std::ffi::c_int,
    _bistate: *mut BulkInsertStateData,
) {
    let relid = (*rel).rd_id;

    let tuple_descriptor = (*slot).tts_tupleDescriptor;
    let natts: usize = (*tuple_descriptor).natts as usize;

    let row = TupleSlot {
        natts,
        datum: std::slice::from_raw_parts_mut((*slot).tts_values, natts),
        nulls: std::slice::from_raw_parts_mut((*slot).tts_isnull, natts),
    };
    insert_table(relid.into(), row);
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_insert_speculative(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _cid: CommandId,
    _options: std::ffi::c_int,
    _bistate: *mut BulkInsertStateData,
    _spec_token: uint32,
) {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_complete_speculative(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _spec_token: uint32,
    _succeeded: bool,
) {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_multi_insert(
    _rel: Relation,
    _slots: *mut *mut TupleTableSlot,
    _nslots: std::ffi::c_int,
    _cid: CommandId,
    _options: std::ffi::c_int,
    _bistate: *mut BulkInsertStateData,
) {
    unimplemented!()
}

#[allow(clippy::too_many_arguments)]
#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_delete(
    _rel: Relation,
    _tid: ItemPointer,
    _cid: CommandId,
    _snapshot: Snapshot,
    _crosscheck: Snapshot,
    _wait: bool,
    _tmfd: *mut TM_FailureData,
    _changing_part: bool,
) -> TM_Result::Type {
    unimplemented!()
}

#[allow(clippy::too_many_arguments)]
#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_update(
    _rel: Relation,
    _otid: ItemPointer,
    _slot: *mut TupleTableSlot,
    _cid: CommandId,
    _snapshot: Snapshot,
    _crosscheck: Snapshot,
    _wait: bool,
    _tmfd: *mut TM_FailureData,
    _lockmode: *mut LockTupleMode::Type,
    _update_indexes: *mut TU_UpdateIndexes::Type,
) -> TM_Result::Type {
    unimplemented!()
}

#[allow(clippy::too_many_arguments)]
#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_lock(
    _rel: Relation,
    _tid: ItemPointer,
    _snapshot: Snapshot,
    _slot: *mut TupleTableSlot,
    _cid: CommandId,
    _mode: LockTupleMode::Type,
    _wait_policy: LockWaitPolicy::Type,
    _flags: uint8,
    _tmfd: *mut TM_FailureData,
) -> TM_Result::Type {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_finish_bulk_insert(_rel: Relation, _options: std::ffi::c_int) {
    // not needed
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_set_new_filelocator(
    rel: Relation,
    _newrlocator: *const RelFileLocator,
    _persistence: std::ffi::c_char,
    _freeze_xid: *mut TransactionId,
    _minmulti: *mut MultiXactId,
) {
    let relid = (*rel).rd_id;
    create_table(relid.into(), *get_schema_from_relation(rel));
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_nontransactional_truncate(_rel: Relation) {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_copy_data(_rel: Relation, _newrlocator: *const RelFileLocator) {
    unimplemented!()
}

#[allow(clippy::too_many_arguments)]
#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_copy_for_cluster(
    _old_table: Relation,
    _new_table: Relation,
    _old_index: Relation,
    _use_sort: bool,
    _oldest_x_min: TransactionId,
    _xid_cutoff: *mut TransactionId,
    _multi_cutoff: *mut MultiXactId,
    _num_tuples: *mut f64,
    _tups_vacuumed: *mut f64,
    _tups_recently_dead: *mut f64,
) {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_vacuum(
    _rel: Relation,
    _params: *mut VacuumParams,
    _bstrategy: BufferAccessStrategy,
) {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_analyze_next_block(
    _scan: TableScanDesc,
    _blockno: BlockNumber,
    _bstrategy: BufferAccessStrategy,
) -> bool {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_analyze_next_tuple(
    _scan: TableScanDesc,
    _oldest_x_min: TransactionId,
    _liverows: *mut f64,
    _deadrows: *mut f64,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

#[allow(clippy::too_many_arguments)]
#[pg_guard]
unsafe extern "C" fn pg_elephantduck_index_build_range_scan(
    _table_rel: Relation,
    _index_rel: Relation,
    _index_info: *mut IndexInfo,
    _allow_sync: bool,
    _anyvisible: bool,
    _progress: bool,
    _start_blockno: BlockNumber,
    _numblocks: BlockNumber,
    _callback: IndexBuildCallback,
    _callback_state: *mut std::ffi::c_void,
    _scan: TableScanDesc,
) -> f64 {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_index_validate_scan(
    _table_rel: Relation,
    _index_rel: Relation,
    _index_info: *mut IndexInfo,
    _snapshot: Snapshot,
    _state: *mut ValidateIndexState,
) {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_size(_rel: Relation, _for_k_number: ForkNumber::Type) -> uint64 {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_needs_toast_table(_rel: Relation) -> bool {
    false // No need to create a toast table.
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_toast_am(_rel: Relation) -> Oid {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_fetch_toast_slice(
    _toastrel: Relation,
    _valueid: Oid,
    _attrsize: int32,
    _sliceoffset: int32,
    _slicelength: int32,
    _result: *mut varlena,
) {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_estimate_size(
    _rel: Relation,
    _attr_widths: *mut int32,
    _pages: *mut BlockNumber,
    _tuples: *mut f64,
    _allvisfrac: *mut f64,
) {
    // TODO Implement this function.
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_bitmap_next_block(
    _scan: TableScanDesc,
    _tbmres: *mut TBMIterateResult,
) -> bool {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_bitmap_next_tuple(
    _scan: TableScanDesc,
    _tbmres: *mut TBMIterateResult,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_sample_next_block(
    _scan: TableScanDesc,
    _scanstate: *mut SampleScanState,
) -> bool {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_sample_next_tuple(
    _scan: TableScanDesc,
    _scanstate: *mut SampleScanState,
    _slot: *mut TupleTableSlot,
) -> bool {
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_executor_finish_hook(query_desc: *mut QueryDesc) {
    match PREV_EXECUTOR_FINISH_HOOK {
        Some(prev_hook) => {
            prev_hook(query_desc);
        }
        None => {
            standard_ExecutorFinish(query_desc);
        }
    }

    close_tables();
}

unsafe fn search_namelist(list: *mut List) -> *mut List {
    let mut name_list = list;
    while !name_list.is_null() {
        let elements = std::slice::from_raw_parts((*name_list).elements, (*name_list).length as usize);
        if elements.is_empty() {
            return std::ptr::null_mut();
        }
        let name_ptr = elements[0].ptr_value as *mut List;

        if (*name_ptr).type_ == NodeTag::T_String {
            return name_list;
        } else if (*name_ptr).type_ == NodeTag::T_List {
            name_list = name_ptr;
        } else {
            return std::ptr::null_mut();
        }
    }
    std::ptr::null_mut()
}

unsafe fn pg_elephantduck_drop_table(stmt: *mut DropStmt) {
    info!("{:?}", *stmt);

    let objects_ptr = (*stmt).objects;
    if objects_ptr.is_null() {
        return;
    }

    info!("{:?}", (*objects_ptr).length);
    let namelist_ptr = search_namelist(objects_ptr);

    if namelist_ptr.is_null() {
        return;
    }

    let rel = makeRangeVarFromNameList(namelist_ptr);
    let relid = RangeVarGetRelidExtended(
        rel,
        AccessShareLock as i32,
        RVROption::RVR_MISSING_OK,
        None,
        std::ptr::null_mut(),
    );
    if is_elephantduck_table(relid) {
        drop_table(relid.into());
    }
}

static mut PREV_EXECUTOR_FINISH_HOOK: ExecutorFinish_hook_type = None;

#[allow(clippy::too_many_arguments)]
#[pg_guard]
unsafe extern "C" fn pg_elephantduck_process_utility_hook(
    pstmt: *mut PlannedStmt,
    query_string: *const ::core::ffi::c_char,
    read_only_tree: bool,
    context: ProcessUtilityContext::Type,
    params: ParamListInfo,
    query_env: *mut QueryEnvironment,
    dest: *mut DestReceiver,
    qc: *mut QueryCompletion,
) {
    let parsetree = (*pstmt).utilityStmt;
    if !parsetree.is_null() && (*parsetree).type_ == NodeTag::T_DropStmt {
        pg_elephantduck_drop_table(parsetree as *mut DropStmt);
    }

    match PREV_PROCESS_UTILITY_HOOK {
        Some(prev_hook) => {
            prev_hook(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                qc,
            );
        }
        None => {
            standard_ProcessUtility(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                qc,
            );
        }
    }
}

static mut PREV_PROCESS_UTILITY_HOOK: ProcessUtility_hook_type = None;

pub fn is_elephantduck_table(relid: Oid) -> bool {
    if relid == InvalidOid {
        return false;
    }

    unsafe {
        let rel = RelationIdGetRelation(relid);
        let result = (*rel).rd_tableam == ELEPHANTDUCK_AM_ROUTINE.lock().unwrap().get_routines();
        RelationClose(rel);
        result
    }
}

pub fn init_tam_hooks() {
    unsafe {
        PREV_EXECUTOR_FINISH_HOOK = ExecutorFinish_hook;
        ExecutorFinish_hook = Some(pg_elephantduck_executor_finish_hook);

        PREV_PROCESS_UTILITY_HOOK = ProcessUtility_hook;
        ProcessUtility_hook = Some(pg_elephantduck_process_utility_hook);
    }
}

pub fn finish_tam_hooks() {
    unsafe {
        ExecutorFinish_hook = PREV_EXECUTOR_FINISH_HOOK;
        ProcessUtility_hook = PREV_PROCESS_UTILITY_HOOK;
    }
}
