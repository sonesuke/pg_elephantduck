use arrow::datatypes::{Field, Fields, Schema as ArrowSchema};
use pgrx::pg_sys::{self, TupleTableSlot};
use pgrx::PgRelation;

use std::path::PathBuf;

use crate::datatype_util::*;
use crate::settings::get_elephantduck_path;

#[derive(Debug, Clone)]
pub struct Attribute {
    pub column_id: i16,
    pub data_type: pg_sys::Oid,
}

#[derive(Clone)]
pub struct Schema {
    pub relation: PgRelation,
    pub fields: Vec<Attribute>,
    pub where_clause: Option<String>,
    pub sample_clause: Option<String>,
}

impl Schema {
    pub fn new(
        relation: PgRelation,
        columns: Vec<i16>,
        where_clause: Option<std::string::String>,
        sample_clause: Option<std::string::String>,
    ) -> Self {
        let tuple_desc = relation.tuple_desc();
        let fields = match columns.len() {
            0 => tuple_desc
                .iter()
                .map(|a| Attribute {
                    column_id: a.attnum,
                    data_type: a.atttypid,
                })
                .collect::<Vec<_>>(),
            _ => columns
                .iter()
                .map(|column| {
                    let attr = tuple_desc.iter().find(|a| a.attnum == *column);
                    match attr {
                        Some(a) => Attribute {
                            column_id: a.attnum,
                            data_type: a.atttypid,
                        },
                        None => {
                            if *column == pg_sys::SelfItemPointerAttributeNumber as i16 {
                                Attribute {
                                    column_id: pg_sys::SelfItemPointerAttributeNumber as i16,
                                    data_type: pg_sys::TIDOID,
                                }
                            } else {
                                panic!("Column not found: {}", column);
                            }
                        }
                    }
                })
                .collect::<Vec<_>>(),
        };

        Self {
            relation: relation.clone(),
            fields,
            where_clause,
            sample_clause,
        }
    }

    pub fn get_where_clause(&self) -> Option<std::string::String> {
        match &self.where_clause {
            Some(where_clause) => {
                if !where_clause.is_empty() {
                    Some(where_clause.clone())
                } else {
                    None
                }
            }
            None => None,
        }
    }

    pub fn get_sample_clause(&self) -> Option<std::string::String> {
        match &self.sample_clause {
            Some(sample_clause) => {
                if !sample_clause.is_empty() {
                    Some(sample_clause.clone())
                } else {
                    None
                }
            }
            None => None,
        }
    }

    pub fn get_pg_types(&self) -> Vec<pg_sys::Oid> {
        self.fields.iter().map(|attr| attr.data_type).collect()
    }

    pub fn get_path(&self) -> String {
        let dir = get_elephantduck_path().unwrap().to_str().unwrap();
        let mut path = PathBuf::from(dir);
        path.push(format!("table_{}.parquet", u32::from(self.relation.oid())));
        path.to_str().unwrap().to_string()
    }

    pub fn get_arrow_schema(&self) -> ArrowSchema {
        let fields: Fields = self
            .fields
            .iter()
            .map(|attr| {
                Field::new(
                    match attr.column_id as i32 {
                        pg_sys::SelfItemPointerAttributeNumber => "file_row_number".to_string(),
                        _ => format!("column_{}", attr.column_id),
                    },
                    convert_datatype_pg_to_arrow(attr.data_type),
                    true,
                )
            })
            .collect();
        ArrowSchema::new(fields)
    }
}

pub struct TupleSlot<'a> {
    pub relation: PgRelation,
    pub natts: usize,
    pub datum: &'a mut [pg_sys::Datum],
    pub nulls: &'a mut [bool],
}

impl TupleSlot<'_> {
    pub fn new(relation: PgRelation, tts: TupleTableSlot) -> Self {
        let tuple_descriptor = tts.tts_tupleDescriptor;
        let natts = if tuple_descriptor.is_null() {
            0_usize
        } else {
            unsafe { (*tuple_descriptor).natts as usize }
        };

        Self {
            relation,
            natts,
            datum: unsafe { std::slice::from_raw_parts_mut(tts.tts_values, natts) },
            nulls: unsafe { std::slice::from_raw_parts_mut(tts.tts_isnull, natts) },
        }
    }
}
