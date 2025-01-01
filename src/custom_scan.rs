use pgrx::pg_sys::*;
use pgrx::prelude::*;

use once_cell::sync::Lazy;
use std::sync::Mutex;

use crate::storage::*;
use crate::tam::is_elephantduck_table;

use crate::extract_clauses::extract_clauses;

/// Custom scan state for elephantduck tables
struct PgElephantduckScanState {
    css: CustomScanState,
    // add some fields if needed
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_create_custom_scan_state(cscan: *mut CustomScan) -> *mut Node {
    let mut scan_state = Box::new(PgElephantduckScanState {
        css: CustomScanState { ..Default::default() },
    });
    scan_state.css.ss.ps.type_ = NodeTag::T_CustomScanState;
    scan_state.css.flags = (*cscan).flags;
    scan_state.css.methods = ELEPHANTDUCK_CUSTOM_EXEC_METHODS.lock().unwrap().get_methods();

    Box::into_raw(scan_state) as *mut Node
}

fn get_schema_from_relation(
    rel: Relation,
    columns: Vec<i16>,
    where_clause: Option<std::string::String>,
    sample_clause: Option<std::string::String>,
) -> Box<Schema> {
    unsafe {
        let tuple_desc = (*rel).rd_att;
        let natts = (*tuple_desc).natts as usize;
        let attrs = (*tuple_desc).attrs.as_slice(natts);
        match columns.len() {
            0 => Box::new(Schema {
                fields: attrs
                    .iter()
                    .map(|a| Attribute {
                        column_id: a.attnum as u32,
                        data_type: a.atttypid,
                    })
                    .collect::<Vec<_>>(),
                where_clause,
                sample_clause,
            }),
            _ => Box::new(Schema {
                fields: columns
                    .iter()
                    .map(|column| {
                        let attr = attrs.iter().find(|a| a.attnum == *column);
                        match attr {
                            Some(a) => Attribute {
                                column_id: a.attnum as u32,
                                data_type: a.atttypid,
                            },
                            None => panic!("Column not found"),
                        }
                    })
                    .collect::<Vec<_>>(),
                where_clause,
                sample_clause,
            }),
        }
    }
}

#[pg_guard]
extern "C" fn pg_elephantduck_begin_custom_scan(csstate: *mut CustomScanState, _estate: *mut EState, _eflags: i32) {
    unsafe {
        let elephantduck_scan_state = csstate as *mut PgElephantduckScanState;
        let rel = (*elephantduck_scan_state).css.ss.ss_currentRelation;
        let target_list = (*(*elephantduck_scan_state).css.ss.ps.plan).targetlist;

        let where_clause = match (*elephantduck_scan_state).css.ss.ps.qual.is_null() {
            false => Some(extract_clauses((*(*elephantduck_scan_state).css.ss.ps.qual).expr)),
            true => None,
        };

        let custom_private = (*((*elephantduck_scan_state).css.ss.ps.plan as *mut CustomScan)).custom_private;
        let sample_clause = match custom_private.is_null() {
            false => {
                let elements =
                    std::slice::from_raw_parts((*custom_private).elements, (*custom_private).length as usize);
                if elements.is_empty() {
                    None
                } else {
                    Some(extract_clauses(elements[0].ptr_value as *mut Expr))
                }
            }
            true => None,
        };
        let columns = if target_list.is_null() {
            Vec::<i16>::new()
        } else {
            let elements = std::slice::from_raw_parts((*target_list).elements, (*target_list).length as usize);
            elements
                .iter()
                .map(|e| {
                    let target_entry = e.ptr_value as *const TargetEntry;
                    let var = (*target_entry).expr as *const Var;
                    (*var).varattnosyn
                })
                .collect::<Vec<i16>>()
        };
        set_schema_for_read(
            (*rel).rd_id.into(),
            *get_schema_from_relation(rel, columns, where_clause, sample_clause),
        );
    }
}

#[pg_guard]
extern "C" fn pg_elephantduck_exec_custom_scan(csstate: *mut CustomScanState) -> *mut TupleTableSlot {
    unsafe {
        let elephantduck_scan_state = csstate as *mut PgElephantduckScanState;
        let slot = (*elephantduck_scan_state).css.ss.ss_ScanTupleSlot;
        let memory_context = (*elephantduck_scan_state).css.ss.ps.ps_ExprContext;

        MemoryContextReset((*memory_context).ecxt_per_tuple_memory);
        ExecClearTuple(slot);

        let old_context = MemoryContextSwitchTo((*memory_context).ecxt_per_tuple_memory);
        let rel = (*elephantduck_scan_state).css.ss.ss_currentRelation;
        let relid = (*rel).rd_id;

        let tuple_descriptor = (*slot).tts_tupleDescriptor;
        let natts: usize = (*tuple_descriptor).natts as usize;
        let mut row = TupleSlot {
            natts,
            datum: std::slice::from_raw_parts_mut((*slot).tts_values, natts),
            nulls: std::slice::from_raw_parts_mut((*slot).tts_isnull, natts),
        };

        MemoryContextSwitchTo(old_context);
        if read(relid.into(), &mut row) {
            ExecStoreVirtualTuple(slot);
            slot
        } else {
            std::ptr::null_mut()
        }
    }
}

#[pg_guard]
extern "C" fn pg_elephantduck_end_custom_scan(csstate: *mut CustomScanState) {
    unsafe {
        if !csstate.is_null() {
            let elephantduck_scan_state = csstate as *mut PgElephantduckScanState;
            let scan_descriptor = (*elephantduck_scan_state).css.ss.ss_currentScanDesc;
            let memory_context = (*elephantduck_scan_state).css.ss.ps.ps_ExprContext;
            let slot = (*elephantduck_scan_state).css.ss.ss_ScanTupleSlot;
            MemoryContextReset((*memory_context).ecxt_per_tuple_memory);
            ExecClearTuple(slot);

            let custom_scan = (*elephantduck_scan_state).css.ss.ps.plan as *mut CustomScan;
            if !(*custom_scan).custom_private.is_null() {
                list_free((*custom_scan).custom_private);
            }

            // I cannot understand why this line is make a server termination
            // let _ = Box::from_raw(csstate as *mut PgElephantduckScanState);
            if !scan_descriptor.is_null() {
                table_endscan(scan_descriptor);
            }
        }
    }
}

#[pg_guard]
extern "C" fn pg_elephantduck_rescan_custom_scan(_csstate: *mut CustomScanState) {
    // Nothing to do
}

/// Custom scan methods for elephantduck tables
struct PgElephantDuckCustomScanMethods {
    methods: CustomScanMethods,
}

/// Implement the custom scan methods for elephantduck tables
impl PgElephantDuckCustomScanMethods {
    /// constructor
    fn new() -> Self {
        PgElephantDuckCustomScanMethods {
            methods: CustomScanMethods {
                CustomName: std::ffi::CString::new("pg_elephantduck_custom_scan")
                    .unwrap()
                    .into_raw(),
                CreateCustomScanState: Some(pg_elephantduck_create_custom_scan_state),
            },
        }
    }

    /// Get raw pointer for PostgreSQL IF
    fn get_methods(&self) -> *mut CustomScanMethods {
        &self.methods as *const _ as *mut _
    }
}

/// The singleton instance of the custom scan methods for elephantduck tables
static mut ELEPHANTDUCK_CUSTOM_SCAN_METHODS: Lazy<Mutex<PgElephantDuckCustomScanMethods>> =
    Lazy::new(|| Mutex::new(PgElephantDuckCustomScanMethods::new()));

/// Custom exec methods for elephantduck tables
struct PgElephantDuckCustomExecMethods {
    methods: CustomExecMethods,
}

/// Implement the custom exec methods for elephantduck tables
impl PgElephantDuckCustomExecMethods {
    /// constructor
    fn new() -> Self {
        PgElephantDuckCustomExecMethods {
            methods: CustomExecMethods {
                CustomName: std::ffi::CString::new("pg_elephantduck_custom_scan")
                    .unwrap()
                    .into_raw(),
                BeginCustomScan: Some(pg_elephantduck_begin_custom_scan),
                ExecCustomScan: Some(pg_elephantduck_exec_custom_scan),
                EndCustomScan: Some(pg_elephantduck_end_custom_scan),
                ReScanCustomScan: Some(pg_elephantduck_rescan_custom_scan),
                MarkPosCustomScan: None,
                RestrPosCustomScan: None,
                EstimateDSMCustomScan: None,
                InitializeDSMCustomScan: None,
                ReInitializeDSMCustomScan: None,
                InitializeWorkerCustomScan: None,
                ShutdownCustomScan: None,
                ExplainCustomScan: None,
            },
        }
    }

    /// Get raw pointer for PostgreSQL IF
    fn get_methods(&self) -> *mut CustomExecMethods {
        &self.methods as *const _ as *mut _
    }
}

/// The singleton instance of the custom exec methods for elephantduck tables
static mut ELEPHANTDUCK_CUSTOM_EXEC_METHODS: Lazy<Mutex<PgElephantDuckCustomExecMethods>> =
    Lazy::new(|| Mutex::new(PgElephantDuckCustomExecMethods::new()));

// fn dump_restrict_info(restrict_info: *mut RestrictInfo) {
//     info!("restrict_info: {:?}", (*restrict_info).clause_relids);
// }

/// Extract actual clauses from a list of clauses
///
/// * `rel` - List. The list of clauses.
/// * `best_path` - CustomPath that is selected as the lowest cost path.
/// * `tlist` - List. The target list of the relation.
/// * `clauses` - List. The list of clauses.
#[pg_guard]
unsafe extern "C" fn pg_elephantduck_plan_custom_path(
    _root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    best_path: *mut CustomPath, // We already removed the path from the list of paths except for elephantduck custom path.
    tlist: *mut List,
    clauses: *mut List,
    _custom_plans: *mut List,
) -> *mut Plan {
    let custom_scan: *mut CustomScan = palloc0(std::mem::size_of::<CustomScan>()) as *mut CustomScan;
    (*(custom_scan as *mut Node)).type_ = NodeTag::T_CustomScan;

    (*custom_scan).methods = ELEPHANTDUCK_CUSTOM_SCAN_METHODS.lock().unwrap().get_methods();

    (*custom_scan).custom_scan_tlist = tlist;
    (*custom_scan).scan.scanrelid = (*rel).relid;
    (*custom_scan).scan.plan.targetlist = tlist;

    (*custom_scan).scan.plan.qual = extract_actual_clauses(clauses, false);
    (*custom_scan).custom_private = (*best_path).custom_private;

    &mut ((*custom_scan).scan.plan) as *mut Plan
}

/// Custom path methods for elephantduck tables
struct PgElephantduckPathMethods {
    methods: CustomPathMethods,
}

/// Implement the custom path methods for elephantduck tables
impl PgElephantduckPathMethods {
    /// constructor
    fn new() -> Self {
        PgElephantduckPathMethods {
            methods: CustomPathMethods {
                CustomName: std::ffi::CString::new("pg_elephantduck_custom_scan")
                    .unwrap()
                    .into_raw(),
                PlanCustomPath: Some(pg_elephantduck_plan_custom_path),
                ReparameterizeCustomPathByChild: None,
            },
        }
    }

    /// Get raw pointer for PostgreSQL IF
    fn get_methods(&self) -> *mut CustomPathMethods {
        &self.methods as *const _ as *mut _
    }
}

/// The singleton instance of the custom path methods for elephantduck tables
static mut ELEPHANTDUCK_CUSTOM_PATH_METHODS: Lazy<Mutex<PgElephantduckPathMethods>> =
    Lazy::new(|| Mutex::new(PgElephantduckPathMethods::new()));

/// Hook function for set rel pathlist
///
/// This function is called when the planner sets the pathlist of a relation.
/// It adds a custom path for elephantduck tables.
///
/// * `root` - PlannerInfo. Not used in this function.
/// * `rel` - RelOptInfo. The relation to set the pathlist.
/// * `rti` - Index. Not used in this function.
/// * `rte` - RangeTblEntry. The range table entry of the relation.
#[pg_guard]
extern "C" fn pg_elephantduck_set_rel_pathlist(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    rti: Index,
    rte: *mut RangeTblEntry,
) {
    unsafe {
        // Call the previous set_rel_pathlist hook for PostgreSQL manner
        if let Some(prev_hook) = PREV_SET_REL_PATHLIST_HOOK {
            prev_hook(root, rel, rti, rte);
        }

        // Check if the relation is a base relation
        if (*rte).relid == InvalidOid || (*rte).rtekind != RTEKind::RTE_RELATION || (*rte).inh {
            return;
        }

        // Remove exists paths, set a custom path for elephantduck tables
        if is_elephantduck_table((*rte).relid) {
            // Remove exists paths
            (*rel).pathlist = std::ptr::null_mut();

            // Create a custom path
            let custom_path: *mut CustomPath = palloc0(std::mem::size_of::<CustomPath>()) as *mut CustomPath;
            (*custom_path).path.type_ = NodeTag::T_CustomPath;
            (*custom_path).path.pathtype = NodeTag::T_CustomScan;
            (*custom_path).path.parent = rel;
            (*custom_path).path.pathtarget = (*rel).reltarget;
            (*custom_path).path.param_info = get_baserel_parampathinfo(root, rel, (*rel).lateral_relids);
            (*custom_path).flags = 0;
            if (*rte).tablesample.is_null() {
                (*custom_path).custom_private = std::ptr::null_mut();
            } else {
                let tablesample_clause = Box::leak(Box::new(ListCell {
                    ptr_value: copyObjectImpl((*rte).tablesample as *mut core::ffi::c_void),
                }));
                (*custom_path).custom_private = list_make1_impl(NodeTag::T_List, *tablesample_clause);
            }
            (*custom_path).methods = ELEPHANTDUCK_CUSTOM_PATH_METHODS.lock().unwrap().get_methods();

            // TODO calculate cost
            (*custom_path).path.rows = (*rel).rows;
            (*custom_path).path.startup_cost = 0.0;
            (*custom_path).path.total_cost = 0.0;

            add_path(rel, &mut ((*custom_path).path) as *mut Path);
        };
    }
}

/// The previous set_rel_pathlist hook
static mut PREV_SET_REL_PATHLIST_HOOK: Option<
    unsafe extern "C" fn(root: *mut PlannerInfo, rel: *mut RelOptInfo, rti: Index, rte: *mut RangeTblEntry),
> = None;

/// Initialize custom scan
///
/// This function is called when the extension is loaded.
/// It registers custom scan methods and sets a hook to set_rel_pathlist.
pub fn init_custom_scan() {
    unsafe {
        pg_sys::RegisterCustomScanMethods(ELEPHANTDUCK_CUSTOM_SCAN_METHODS.lock().unwrap().get_methods());

        PREV_SET_REL_PATHLIST_HOOK = pg_sys::set_rel_pathlist_hook;
        pg_sys::set_rel_pathlist_hook = Some(pg_elephantduck_set_rel_pathlist);
    }
}

/// Finish custom scan
///
/// This function is called when the extension is unloaded.
/// It resets the hook to set_rel_pathlist.
pub fn finish_custom_scan() {
    unsafe {
        pg_sys::set_rel_pathlist_hook = PREV_SET_REL_PATHLIST_HOOK;
    }
}
