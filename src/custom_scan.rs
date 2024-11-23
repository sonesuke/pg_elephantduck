use pgrx::pg_sys::*;
use pgrx::prelude::*;

use once_cell::sync::Lazy;
use std::sync::Mutex;

use crate::tam::is_elephantduck_table;

/// Custom scan state for elephantduck tables
struct PgElephantduckScanState {
    css: CustomScanState,
    // add some fields if needed
}

#[pg_guard]
unsafe extern "C" fn pg_elephantduck_create_custom_scan_state(cscan: *mut CustomScan) -> *mut Node {
    // Implement the creation of custom scan state
    info!("elephantduck: create custom scan state");
    let scan_state: *mut PgElephantduckScanState =
        palloc0(std::mem::size_of::<PgElephantduckScanState>()) as *mut PgElephantduckScanState;
    (*(scan_state as *mut Node)).type_ = NodeTag::T_CustomScanState;
    (*scan_state).css.flags = (*cscan).flags;
    (*scan_state).css.methods = ELEPHANTDUCK_CUSTOM_EXEC_METHODS.lock().unwrap().get_methods();

    (&(*scan_state).css as *const _) as *mut Node
}

#[pg_guard]
extern "C" fn pg_elephantduck_begin_custom_scan(_csstate: *mut CustomScanState, _estate: *mut EState, _eflags: i32) {
    info!("elephantduck: begin custom scan");
    unimplemented!()
}

#[pg_guard]
extern "C" fn pg_elephantduck_exec_custom_scan(_csstate: *mut CustomScanState) -> *mut TupleTableSlot {
    info!("elephantduck: exec custom scan");
    unimplemented!()
}

#[pg_guard]
extern "C" fn pg_elephantduck_end_custom_scan(_csstate: *mut CustomScanState) {
    info!("elephantduck: end custom scan");
    unimplemented!()
}

#[pg_guard]
extern "C" fn pg_elephantduck_rescan_custom_scan(_csstate: *mut CustomScanState) {
    info!("elephantduck: rescan custom scan");
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
    info!("elephantduck: plan custom path");

    let custom_scan: *mut CustomScan = palloc0(std::mem::size_of::<CustomScan>()) as *mut CustomScan;
    (*(custom_scan as *mut Node)).type_ = NodeTag::T_CustomScan;

    (*custom_scan).methods = ELEPHANTDUCK_CUSTOM_SCAN_METHODS.lock().unwrap().get_methods();

    (*custom_scan).scan.scanrelid = (*rel).relid;
    (*custom_scan).scan.plan.targetlist = tlist;
    (*custom_scan).scan.plan.qual = extract_actual_clauses(clauses, false);
    (*custom_scan).custom_exprs = extract_actual_clauses((*best_path).custom_private, false);

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
        info!("elephantduck: set rel pathlist");

        // Call the previous set_rel_pathlist hook for PostgreSQL manner
        if let Some(prev_hook) = PREV_SET_REL_PATHLIST_HOOK {
            prev_hook(root, rel, rti, rte);
        }

        // Check if the relation is a base relation
        if (*rte).relid == InvalidOid || (*rte).rtekind != RTEKind::RTE_RELATION || (*rte).inh {
            info!("elephantduck: return because of invalid rte");
            return;
        }

        // Remove exists paths, set a custom path for elephantduck tables
        if is_elephantduck_table((*rte).relid) {
            info!("elephantduck: set rel pathlist for elephantduck table");

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
            (*custom_path).custom_private = std::ptr::null_mut();
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
        info!("elephantduck: init custom scan");
        pg_sys::RegisterCustomScanMethods(ELEPHANTDUCK_CUSTOM_SCAN_METHODS.lock().unwrap().get_methods());

        PREV_SET_REL_PATHLIST_HOOK = pg_sys::set_rel_pathlist_hook;
        // pg_sys::set_rel_pathlist_hook = Some(pg_elephantduck_set_rel_pathlist);
    }
}

/// Finish custom scan
///
/// This function is called when the extension is unloaded.
/// It resets the hook to set_rel_pathlist.
pub fn finish_custom_scan() {
    unsafe {
        info!("elephantduck: finish custom scan");
        // pg_sys::set_rel_pathlist_hook = PREV_SET_REL_PATHLIST_HOOK;
    }
}
