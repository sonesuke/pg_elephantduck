use pgrx::pg_sys::{self, *};
use pgrx::*;
use std::ffi::CStr;

fn extract_var(var: *mut Var) -> std::string::String {
    unsafe {
        match (*var).varattnosyn as i32 {
            pg_sys::SelfItemPointerAttributeNumber => "file_row_number".to_string(),
            _ => format!("column_{}", (*var).varattnosyn),
        }
    }
}

fn extract_bool_expr(bool_expr: *mut BoolExpr) -> std::string::String {
    unsafe {
        let args = (*bool_expr).args;
        let elements = std::slice::from_raw_parts((*args).elements, (*args).length as usize);
        let expressions = elements
            .iter()
            .map(|element| format!("({})", extract_clauses(element.ptr_value as *mut Expr)).to_string())
            .collect::<Vec<_>>();

        match (*bool_expr).boolop {
            BoolExprType::AND_EXPR => expressions.join(" AND "),
            BoolExprType::OR_EXPR => expressions.join(" OR "),
            BoolExprType::NOT_EXPR => {
                let condition = expressions.first().map_or("", |s| s);
                format!("NOT ({})", condition).to_string()
            }
            _ => "".to_string(),
        }
    }
}

fn extract_op_expr(op_expr: *mut OpExpr) -> std::string::String {
    unsafe {
        let opname = CStr::from_ptr(get_opname((*op_expr).opno))
            .to_string_lossy()
            .into_owned();
        let args = (*op_expr).args;
        let elements = std::slice::from_raw_parts((*args).elements, (*args).length as usize);
        let expressions = elements
            .iter()
            .map(|element| format!("({})", extract_clauses(element.ptr_value as *mut Expr)).to_string())
            .collect::<Vec<_>>();

        match opname.as_str() {
            "=" => {
                let left = expressions.first().unwrap();
                let right = expressions.last().unwrap();
                format!("{} = {}", left, right).to_string()
            }
            "<>" => {
                let left = expressions.first().unwrap();
                let right = expressions.last().unwrap();
                format!("{} <> {}", left, right).to_string()
            }
            "<" => {
                let left = expressions.first().unwrap();
                let right = expressions.last().unwrap();
                format!("{} < {}", left, right).to_string()
            }
            "<=" => {
                let left = expressions.first().unwrap();
                let right = expressions.last().unwrap();
                format!("{} <= {}", left, right).to_string()
            }
            ">" => {
                let left = expressions.first().unwrap();
                let right = expressions.last().unwrap();
                format!("{} > {}", left, right).to_string()
            }
            ">=" => {
                let left = expressions.first().unwrap();
                let right = expressions.last().unwrap();
                format!("{} >= {}", left, right).to_string()
            }
            _ => "".to_string(),
        }
    }
}

fn extract_const_expr(const_expr: *mut Const) -> std::string::String {
    unsafe {
        let value = (*const_expr).constvalue;
        let isnull = (*const_expr).constisnull;
        match (*const_expr).consttype {
            pg_sys::BOOLOID => match bool::from_datum(value, isnull) {
                Some(result) => result.to_string(),
                None => "".to_string(),
            },
            pg_sys::INT2OID => match i16::from_datum(value, isnull) {
                Some(result) => result.to_string(),
                None => "".to_string(),
            },
            pg_sys::INT4OID => match i32::from_datum(value, isnull) {
                Some(result) => result.to_string(),
                None => "".to_string(),
            },
            pg_sys::INT8OID => match i64::from_datum(value, isnull) {
                Some(result) => result.to_string(),
                None => "".to_string(),
            },
            pg_sys::FLOAT4OID => match f32::from_datum(value, isnull) {
                Some(result) => result.to_string(),
                None => "".to_string(),
            },
            pg_sys::FLOAT8OID => match f64::from_datum(value, isnull) {
                Some(result) => result.to_string(),
                None => "".to_string(),
            },
            pg_sys::DATEOID => match pgrx::datum::Date::from_datum(value, isnull) {
                Some(result) => format!("'{}'", result),
                None => "".to_string(),
            },
            pg_sys::TIMEOID => match pgrx::datum::Time::from_datum(value, isnull) {
                Some(result) => format!("'{}'", result),
                None => "".to_string(),
            },
            pg_sys::TIMESTAMPOID => match pgrx::datum::Timestamp::from_datum(value, isnull) {
                Some(result) => format!("'{}'", result),
                None => "".to_string(),
            },
            pg_sys::TEXTOID => match std::string::String::from_datum(value, isnull) {
                Some(result) => format!("'{}'", result),
                None => "".to_string(),
            },
            _ => "".to_string(),
        }
    }
}

fn extract_null_test(null_test: *mut NullTest) -> std::string::String {
    unsafe {
        let arg = (*null_test).arg;
        match (*null_test).nulltesttype {
            NullTestType::IS_NULL => format!("{} IS NULL", extract_clauses(arg)),
            NullTestType::IS_NOT_NULL => format!("{} IS NOT NULL", extract_clauses(arg)),
            _ => "".to_string(),
        }
    }
}

fn extract_list(list: *mut List) -> std::string::String {
    unsafe {
        let elements = std::slice::from_raw_parts((*list).elements, (*list).length as usize);
        match elements.len() {
            0 => "".to_string(),
            1 => extract_clauses(elements[0].ptr_value as *mut Expr),
            _ => {
                let expressions = elements
                    .iter()
                    .map(|element| format!("({})", extract_clauses(element.ptr_value as *mut Expr)).to_string())
                    .collect::<Vec<_>>();
                expressions.join(" AND ")
            }
        }
    }
}

fn extract_tablesample(tablesample: *mut TableSampleClause) -> std::string::String {
    unsafe {
        let args_ptr = (*tablesample).args;
        let args = std::slice::from_raw_parts((*args_ptr).elements, (*args_ptr).length as usize);
        let repeatable = (*tablesample).repeatable;
        let args = args
            .iter()
            .map(|e| extract_clauses(e.ptr_value as *mut Expr))
            .collect::<Vec<std::string::String>>()
            .join(", ");
        match repeatable.is_null() {
            true => format!("USING SAMPLE {} PERCENT (bernoulli)", args),
            false => {
                let repeatable = extract_clauses(repeatable);
                format!("USING SAMPLE {} PERCENT (bernoulli, {})", args, repeatable)
            }
        }
    }
}

pub fn extract_clauses(expr: *mut Expr) -> std::string::String {
    unsafe {
        match (*expr).type_ {
            NodeTag::T_List => extract_list(expr as *mut List),
            NodeTag::T_Var => extract_var(expr as *mut Var),
            NodeTag::T_OpExpr => extract_op_expr(expr as *mut OpExpr),
            NodeTag::T_BoolExpr => extract_bool_expr(expr as *mut BoolExpr),
            NodeTag::T_NullTest => extract_null_test(expr as *mut NullTest),
            NodeTag::T_Const => extract_const_expr(expr as *mut Const),
            NodeTag::T_TableSampleClause => extract_tablesample(expr as *mut TableSampleClause),
            _ => {
                panic!("Unknown expression type: {:?}", (*expr).type_);
            }
        }
    }
}
