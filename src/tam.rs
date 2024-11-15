// table_ access method (TM) interface

use std::collections::HashMap;
use std::vec;

use pgrx::info;
use pgrx::pg_sys::*;

#[allow(unused_imports)]
use pgrx::prelude::*;

// The handler function for the access method.
// This function is called when the access method is created.
#[pg_guard]
#[no_mangle]
pub extern "C" fn pg_elephantduck_handler(_fcinfo: FunctionCallInfo) -> *mut TableAmRoutine {
    let table_am_routine = Box::new(TableAmRoutine {
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
    });

    Box::into_raw(table_am_routine)
}

static mut VIRTUAL_TABLE: std::sync::LazyLock<std::sync::Mutex<HashMap<std::string::String, Vec<Vec<i32>>>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_slot_callbacks(_rel: Relation) -> *const TupleTableSlotOps {
    info!("pg_elephantduck_slot_callbacks is called");
    // Minimal Implement.
    // See https://github.com/postgres/postgres/blob/master/src/include/executor/tuptable.h#L33
    &TTSOpsVirtual
}

#[allow(dead_code)]
pub struct ElepantDuckScan {
    rs_base: TableScanDescData, // Base class from access/relscan.h.
    index: usize,
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
    info!("pg_elephantduck_scan_begin is called");
    let scan = Box::new(ElepantDuckScan {
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
        index: 0,
    });
    Box::into_raw(scan) as TableScanDesc
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_end(scan: TableScanDesc) {
    info!("pg_elephantduck_scan_end is called");
    if !scan.is_null() {
        let _ = Box::from_raw(scan as *mut ElepantDuckScan);
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
    info!("pg_elephantduck_scan_rescan is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_getnextslot(
    scan: TableScanDesc,
    _direction: ScanDirection::Type,
    slot: *mut TupleTableSlot,
) -> bool {
    info!("pg_elephantduck_scan_getnextslot is called");

    let elephant_duck_scan = scan as *mut ElepantDuckScan;

    let rel = (*elephant_duck_scan).rs_base.rs_rd;
    let name = name_data_to_str(&(*(*rel).rd_rel).relname);
    let _namespace = (*(*rel).rd_rel).relnamespace.as_u32();

    let tables = VIRTUAL_TABLE.lock().unwrap();
    let table = tables.get(name).unwrap();

    ExecClearTuple(slot);

    if (*elephant_duck_scan).index >= table[0].len() {
        return false;
    }

    (*slot).tts_nvalid = table.len() as i16;
    let values = std::slice::from_raw_parts_mut((*slot).tts_values, (*slot).tts_nvalid as usize);
    let isnull = std::slice::from_raw_parts_mut((*slot).tts_isnull, (*slot).tts_nvalid as usize);
    for i in 0..table.len() {
        values[i] = Int32GetDatum(table[i][(*elephant_duck_scan).index]);
        isnull[i] = false;
    }

    ExecStoreVirtualTuple(slot);
    (*elephant_duck_scan).index += 1;
    true
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_set_tidrange(
    _scan: TableScanDesc,
    _mintid: ItemPointer,
    _maxtid: ItemPointer,
) {
    info!("pg_elephantduck_scan_set_tidrange is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_getnextslot_tidrange(
    _scan: TableScanDesc,
    _direction: ScanDirection::Type,
    _slot: *mut TupleTableSlot,
) -> bool {
    info!("pg_elephantduck_scan_getnextslot_tidrange is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_parallelscan_estimate(_rel: Relation) -> Size {
    info!("pg_elephantduck_parallelscan_estimate is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_parallelscan_initialize(_rel: Relation, _pscan: ParallelTableScanDesc) -> Size {
    info!("pg_elephantduck_parallelscan_initialize is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_parallelscan_reinitialize(_rel: Relation, _pscan: ParallelTableScanDesc) {
    info!("pg_elephantduck_parallelscan_reinitialize is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_index_fetch_begin(_rel: Relation) -> *mut IndexFetchTableData {
    info!("pg_elephantduck_index_fetch_begin is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_index_fetch_reset(_data: *mut IndexFetchTableData) {
    info!("pg_elephantduck_index_fetch_reset is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_index_fetch_end(_data: *mut IndexFetchTableData) {
    info!("pg_elephantduck_index_fetch_end is called");
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
    info!("pg_elephantduck_index_fetch_tuple is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_fetch_row_version(
    _rel: Relation,
    _tid: ItemPointer,
    _snapshot: Snapshot,
    _slot: *mut TupleTableSlot,
) -> bool {
    info!("pg_elephantduck_tuple_fetch_row_version is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_tid_valid(_scan: TableScanDesc, _tid: ItemPointer) -> bool {
    info!("pg_elephantduck_tuple_tid_valid is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_get_latest_tid(_scan: TableScanDesc, _tid: ItemPointer) {
    info!("pg_elephantduck_tuple_get_latest_tid is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_satisfies_snapshot(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _snapshot: Snapshot,
) -> bool {
    info!("pg_elephantduck_tuple_satisfies_snapshot is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_index_delete_tuples(
    _rel: Relation,
    _delstate: *mut TM_IndexDeleteOp,
) -> TransactionId {
    info!("pg_elephantduck_index_delete_tuples is called");
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
    info!("pg_elephantduck_tuple_insert is called");

    let name = name_data_to_str(&(*(*rel).rd_rel).relname);
    let _namespace = &(*(*rel).rd_rel).relnamespace.as_u32();

    let mut tables = VIRTUAL_TABLE.lock().unwrap();

    let table = match tables.get_mut(name) {
        Some(table) => table,
        None => {
            // Handle the error, e.g., return or log an error
            // This will be removed in the future.
            return;
        }
    };

    let nvalid = (*slot).tts_nvalid as usize;
    let values = std::slice::from_raw_parts((*slot).tts_values, nvalid);
    let isnull = std::slice::from_raw_parts((*slot).tts_isnull, nvalid);

    for i in 0..nvalid {
        let value = i32::from_datum(values[i], false).map_or(0, |d| d);
        let isnull = isnull[i];

        if isnull {
            continue;
        }

        table[i].push(value);
    }
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
    info!("pg_elephantduck_tuple_insert_speculative is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_tuple_complete_speculative(
    _rel: Relation,
    _slot: *mut TupleTableSlot,
    _spec_token: uint32,
    _succeeded: bool,
) {
    info!("pg_elephantduck_tuple_complete_speculative is called");
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
    info!("pg_elephantduck_multi_insert is called");
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
    info!("pg_elephantduck_tuple_delete is called");
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
    info!("pg_elephantduck_tuple_update is called");
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
    info!("pg_elephantduck_tuple_lock is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_finish_bulk_insert(_rel: Relation, _options: std::ffi::c_int) {
    info!("pg_elephantduck_finish_bulk_insert is called");
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
    info!("pg_elephantduck_relation_set_new_filelocator is called");

    let name = name_data_to_str(&(*(*rel).rd_rel).relname);
    let namespace = &(*(*rel).rd_rel).relnamespace.as_u32();

    let tuple_desc = (*rel).rd_att;
    let natts = (*tuple_desc).natts as usize;
    let attrs = (*tuple_desc).attrs.as_slice(natts);

    for attr in attrs.iter().take(natts) {
        if attr.is_dropped() {
            continue;
        }

        let att_name = attr.name();
        let att_type_oid = attr.type_oid();
        let att_num = attr.num();

        info!("name: {}, type: {:?}, num: {}", att_name, att_type_oid, att_num);
    }

    let mut table = VIRTUAL_TABLE.lock().unwrap();
    table.insert(name.to_string(), vec![vec![]; natts]);

    info!("namespace: {}, name: {}", namespace, name);
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_nontransactional_truncate(_rel: Relation) {
    info!("pg_elephantduck_relation_nontransactional_truncate is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_copy_data(_rel: Relation, _newrlocator: *const RelFileLocator) {
    info!("pg_elephantduck_relation_copy_data is called");
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
    info!("pg_elephantduck_relation_copy_for_cluster is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_vacuum(
    _rel: Relation,
    _params: *mut VacuumParams,
    _bstrategy: BufferAccessStrategy,
) {
    info!("pg_elephantduck_relation_vacuum is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_analyze_next_block(
    _scan: TableScanDesc,
    _blockno: BlockNumber,
    _bstrategy: BufferAccessStrategy,
) -> bool {
    info!("pg_elephantduck_scan_analyze_next_block is called");
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
    info!("pg_elephantduck_scan_analyze_next_tuple is called");
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
    info!("pg_elephantduck_index_build_range_scan is called");
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
    info!("pg_elephantduck_index_validate_scan is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_size(_rel: Relation, _for_k_number: ForkNumber::Type) -> uint64 {
    info!("pg_elephantduck_relation_size is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_needs_toast_table(_rel: Relation) -> bool {
    // info!("pg_elephantduck_relation_needs_toast_table is called");
    false // No need to create a toast table.
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_relation_toast_am(_rel: Relation) -> Oid {
    info!("pg_elephantduck_relation_toast_am is called");
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
    info!("pg_elephantduck_relation_fetch_toast_slice is called");
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
    info!("pg_elephantduck_relation_estimate_size is called");
    // TODO Implement this function.
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_bitmap_next_block(
    _scan: TableScanDesc,
    _tbmres: *mut TBMIterateResult,
) -> bool {
    info!("pg_elephantduck_scan_bitmap_next_block is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_bitmap_next_tuple(
    _scan: TableScanDesc,
    _tbmres: *mut TBMIterateResult,
    _slot: *mut TupleTableSlot,
) -> bool {
    info!("pg_elephantduck_scan_bitmap_next_tuple is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_sample_next_block(
    _scan: TableScanDesc,
    _scanstate: *mut SampleScanState,
) -> bool {
    info!("pg_elephantduck_scan_sample_next_block is called");
    unimplemented!()
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_scan_sample_next_tuple(
    _scan: TableScanDesc,
    _scanstate: *mut SampleScanState,
    _slot: *mut TupleTableSlot,
) -> bool {
    info!("pg_elephantduck_scan_sample_next_tuple is called");
    unimplemented!()
}
