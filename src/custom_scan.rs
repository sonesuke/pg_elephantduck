use pgrx::pg_sys::*;
use pgrx::prelude::*;

use once_cell::sync::Lazy;

use pgrx::PgRelation;
use std::sync::Mutex;

use crate::pg_utils::IsElephantDuckTable;
use pgrx::list::PgList;

use crate::common::*;
use crate::storage::*;

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

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_begin_custom_scan(
    csstate: *mut CustomScanState,
    _estate: *mut EState,
    _eflags: i32,
) {
    let custom_private = PgList::<Expr>::from_pg((*((*csstate).ss.ps.plan as *mut CustomScan)).custom_private);

    let where_clause = match custom_private.head() {
        Some(cell) if !cell.is_null() => Some(extract_clauses(cell)),
        _ => None,
    };
    let sample_clause = match custom_private.tail() {
        Some(cell) if custom_private.len() > 1 && !cell.is_null() => Some(extract_clauses(cell)),
        _ => None,
    };

    let selected_columns = PgList::<TargetEntry>::from_pg((*(*csstate).ss.ps.plan).targetlist)
        .iter_ptr()
        .map(|entry| (*((*entry).expr as *const Var)).varattnosyn)
        .collect::<Vec<i16>>();

    let rel = PgRelation::from_pg((*csstate).ss.ss_currentRelation);
    set_schema_for_read(Schema::new(rel, selected_columns, where_clause, sample_clause));
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_exec_custom_scan(csstate: *mut CustomScanState) -> *mut TupleTableSlot {
    let slot = (*csstate).ss.ss_ScanTupleSlot;
    ExecClearTuple(slot);

    let mut row = TupleSlot::new(PgRelation::from_pg((*csstate).ss.ss_currentRelation), *slot);
    if read(&mut row) {
        ExecStoreVirtualTuple(slot);
        slot
    } else {
        std::ptr::null_mut()
    }
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_end_custom_scan(csstate: *mut CustomScanState) {
    if !csstate.is_null() {
        let slot = (*csstate).ss.ss_ScanTupleSlot;
        ExecClearTuple(slot);

        let custom_scan = (*csstate).ss.ps.plan as *mut CustomScan;
        if !(*custom_scan).custom_private.is_null() {
            list_free((*custom_scan).custom_private);
        }

        // I cannot understand why this line is make a server termination
        // let _ = Box::from_raw(csstate as *mut PgElephantduckScanState);
        let scan_descriptor = (*csstate).ss.ss_currentScanDesc;
        if !scan_descriptor.is_null() {
            table_endscan(scan_descriptor);
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
    let mut custom_private = PgList::<List>::new();
    custom_private.push(PgList::<Expr>::from_pg(extract_actual_clauses(clauses, false)).into_pg());

    PgList::<List>::from_pg((*best_path).custom_private)
        .iter_ptr()
        .for_each(|cell| custom_private.push(cell));

    let custom_scan = Box::new(CustomScan {
        scan: Scan {
            plan: Plan {
                type_: NodeTag::T_CustomScan,
                startup_cost: (*best_path).path.startup_cost,
                total_cost: (*best_path).path.total_cost,
                plan_rows: (*best_path).path.rows,
                plan_width: 0,
                parallel_aware: (*best_path).path.parallel_aware,
                parallel_safe: (*best_path).path.parallel_safe,
                async_capable: false,
                plan_node_id: 0,
                targetlist: tlist,
                qual: std::ptr::null_mut(),
                // Use custom_private for transfer quals, becuase it forces to add it to custom_scan_tlist.
                // It constrains the shape of the tuple and it is necessary to allocate memory for the tuple.
                lefttree: std::ptr::null_mut(),
                righttree: std::ptr::null_mut(),
                initPlan: std::ptr::null_mut(),
                extParam: std::ptr::null_mut(),
                allParam: std::ptr::null_mut(),
            },
            scanrelid: (*rel).relid,
        },
        flags: 0,
        custom_plans: std::ptr::null_mut(),
        custom_exprs: std::ptr::null_mut(),
        custom_private: custom_private.into_pg(),
        custom_scan_tlist: tlist,
        custom_relids: std::ptr::null_mut(),
        methods: ELEPHANTDUCK_CUSTOM_SCAN_METHODS.lock().unwrap().get_methods(),
    });

    Box::into_raw(custom_scan) as *mut Plan
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
unsafe extern "C" fn pg_elephantduck_set_rel_pathlist(
    root: *mut PlannerInfo,
    rel: *mut RelOptInfo,
    rti: Index,
    rte: *mut RangeTblEntry,
) {
    // Call the previous set_rel_pathlist hook for PostgreSQL manner
    if let Some(prev_hook) = PREV_SET_REL_PATHLIST_HOOK {
        prev_hook(root, rel, rti, rte);
    }

    // Check if the relation is a base relation
    if (*rte).relid == InvalidOid || (*rte).rtekind != RTEKind::RTE_RELATION || (*rte).inh {
        return;
    }

    // Remove exists paths, set a custom path for elephantduck tables
    if PgRelation::open((*rte).relid).is_elephantduck_table() {
        // Remove exists paths
        (*rel).pathlist = std::ptr::null_mut();

        let mut custom_private = PgList::<TableSampleClause>::new();

        if !(*rte).tablesample.is_null() {
            let tablesample = PgBox::from_pg((*rte).tablesample).clone();
            custom_private.push(tablesample.into_pg());
        }

        // Create a custom path
        let custom_path = Box::new(CustomPath {
            path: Path {
                type_: NodeTag::T_CustomPath,
                pathtype: NodeTag::T_CustomScan,
                parent: rel,
                pathtarget: (*rel).reltarget,
                param_info: get_baserel_parampathinfo(root, rel, (*rel).lateral_relids),
                rows: (*rel).rows,
                startup_cost: 0.0,
                total_cost: 0.0,
                parallel_aware: false,
                parallel_safe: false,
                parallel_workers: 0,
                pathkeys: std::ptr::null_mut(),
            },
            custom_private: custom_private.into_pg(),
            custom_paths: std::ptr::null_mut(),
            flags: 0,
            methods: ELEPHANTDUCK_CUSTOM_PATH_METHODS.lock().unwrap().get_methods(),
        });
        add_path(rel, Box::into_raw(custom_path) as *mut Path);
    };
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
