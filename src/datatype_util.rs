use arrow::array::{Array, ArrayRef};
use pgrx::pg_sys::{self};
use pgrx::prelude::*;
use std::sync::Arc;

use crate::common::*;
use crate::datetime_util::*;

pub fn convert_datatype_pg_to_arrow(data_type_oid: pg_sys::Oid) -> arrow::datatypes::DataType {
    match data_type_oid {
        pg_sys::BOOLOID => arrow::datatypes::DataType::Boolean,
        pg_sys::INT4OID => arrow::datatypes::DataType::Int32,
        pg_sys::INT8OID => arrow::datatypes::DataType::Int64,
        pg_sys::FLOAT4OID => arrow::datatypes::DataType::Float32,
        pg_sys::FLOAT8OID => arrow::datatypes::DataType::Float64,
        pg_sys::DATEOID => arrow::datatypes::DataType::Date32,
        pg_sys::TIMEOID => arrow::datatypes::DataType::Time32(arrow::datatypes::TimeUnit::Second),
        pg_sys::TIMESTAMPOID => arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
        pg_sys::TEXTOID => arrow::datatypes::DataType::Utf8,
        pg_sys::TIDOID => arrow::datatypes::DataType::Int64,
        _ => panic!("Invalid data type {:?}", data_type_oid),
    }
}

pub fn convert_datum_pg_to_arrow(
    data_type_oid: pg_sys::Oid,
    datum: pg_sys::Datum,
    is_null: bool,
) -> arrow::array::ArrayRef {
    unsafe {
        match data_type_oid {
            pg_sys::BOOLOID => {
                Arc::new(arrow::array::BooleanArray::from(vec![bool::from_datum(datum, is_null)])) as ArrayRef
            }
            pg_sys::INT4OID => {
                Arc::new(arrow::array::Int32Array::from(vec![i32::from_datum(datum, is_null)])) as ArrayRef
            }
            pg_sys::INT8OID => {
                Arc::new(arrow::array::Int64Array::from(vec![i64::from_datum(datum, is_null)])) as ArrayRef
            }
            pg_sys::FLOAT4OID => {
                Arc::new(arrow::array::Float32Array::from(vec![f32::from_datum(datum, is_null)])) as ArrayRef
            }
            pg_sys::FLOAT8OID => {
                Arc::new(arrow::array::Float64Array::from(vec![f64::from_datum(datum, is_null)])) as ArrayRef
            }
            pg_sys::DATEOID => Arc::new(arrow::array::Date32Array::from(vec![pgrx::datum::Date::from_datum(
                datum, is_null,
            )
            .unwrap()
            .to_epoch_day()])) as ArrayRef,
            pg_sys::TIMEOID => Arc::new(arrow::array::Time32SecondArray::from(vec![
                pgrx::datum::Time::from_datum(datum, is_null).unwrap().to_epoch_time() as i32,
            ])) as ArrayRef,
            pg_sys::TIMESTAMPOID => Arc::new(arrow::array::TimestampSecondArray::from(vec![
                pgrx::datum::Timestamp::from_datum(datum, is_null)
                    .unwrap()
                    .to_epoch_time(),
            ])) as ArrayRef,
            pg_sys::TEXTOID => Arc::new(arrow::array::StringArray::from(vec![String::from_datum(
                datum, is_null,
            )])) as ArrayRef,
            _ => panic!("Invalid data type {:?}", data_type_oid),
        }
    }
}

pub fn convert_datum_arrow_to_pg(
    field: &ArrayRef,
    column_index: usize,
    pg_type: pg_sys::Oid,
    current_row: usize,
    row: &mut TupleSlot,
) {
    match field.data_type() {
        arrow::datatypes::DataType::Boolean => {
            let array = field.as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
            row.datum[column_index] = array.value(current_row).into_datum().unwrap();
            row.nulls[column_index] = array.is_null(current_row);
        }
        arrow::datatypes::DataType::Int32 => {
            let array = field.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
            row.datum[column_index] = array.value(current_row).into_datum().unwrap();
            row.nulls[column_index] = array.is_null(current_row);
        }
        arrow::datatypes::DataType::Int64 => match pg_type {
            pg_sys::TIDOID => {
                let array = field.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
                let mut tid = unsafe { PgBox::<pg_sys::ItemPointerData>::alloc() };
                tid.ip_blkid.bi_hi = 0;
                tid.ip_blkid.bi_lo = 0;
                tid.ip_posid = array.value(current_row) as u16;
                row.datum[column_index] = tid.into_datum().unwrap();
                row.nulls[column_index] = false;
            }
            _ => {
                let array = field.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
                row.datum[column_index] = array.value(current_row).into_datum().unwrap();
                row.nulls[column_index] = array.is_null(current_row);
            }
        },
        arrow::datatypes::DataType::Float32 => {
            let array = field.as_any().downcast_ref::<arrow::array::Float32Array>().unwrap();
            row.datum[column_index] = array.value(current_row).into_datum().unwrap();
            row.nulls[column_index] = array.is_null(current_row);
        }
        arrow::datatypes::DataType::Float64 => {
            let array = field.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
            row.datum[column_index] = array.value(current_row).into_datum().unwrap();
            row.nulls[column_index] = array.is_null(current_row);
        }
        arrow::datatypes::DataType::Date32 => {
            let array = field.as_any().downcast_ref::<arrow::array::Date32Array>().unwrap();
            row.datum[column_index] = pgrx::datum::Date::from_epoch_day(array.value(current_row))
                .into_datum()
                .unwrap();
            row.nulls[column_index] = array.is_null(current_row);
        }
        arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None) => {
            let array = field
                .as_any()
                .downcast_ref::<arrow::array::TimestampSecondArray>()
                .unwrap();
            row.datum[column_index] = pgrx::datum::Timestamp::from_epoch_time(array.value(current_row))
                .into_datum()
                .unwrap();
            row.nulls[column_index] = array.is_null(current_row);
        }
        arrow::datatypes::DataType::Utf8 => {
            let array = field.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            row.datum[column_index] = array.value(current_row).into_datum().unwrap();
            row.nulls[column_index] = array.is_null(current_row);
        }
        _ => panic!("Invalid data type {:?}", field.data_type()),
    }
}
