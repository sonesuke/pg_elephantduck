use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

use duckdb::{ArrowStream, Config, Connection, Statement};

use pgrx::pg_sys::{self};

use crate::common::*;
use crate::datatype_util::*;
use crate::settings::get_elephantduck_threads;

pub struct DuckdbReader {
    statement: &'static mut Statement<'static>,
    arrow_stream: &'static mut ArrowStream<'static>,
    record_batch: Option<RecordBatch>,
    pg_types: Option<Vec<pg_sys::Oid>>,
    current_row: usize,
}

fn get_columns_clause(arrow_schema: &ArrowSchema) -> String {
    arrow_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<&str>>()
        .join(", ")
}

impl DuckdbReader {
    pub fn new(schema: Schema) -> Self {
        let file_path = schema.get_path();
        let arrow_schema = schema.get_arrow_schema();
        let columns_clause = get_columns_clause(&arrow_schema);
        let file_row_number_clause = ", file_row_number = true"; // TODO: Use a better way to detect this
        let mut sql = match schema.get_where_clause() {
            Some(where_clause) => format!(
                "SELECT {} FROM parquet_scan('{}'{}) WHERE {}",
                columns_clause, file_path, file_row_number_clause, where_clause
            ),
            None => format!(
                "SELECT {} FROM parquet_scan('{}'{})",
                columns_clause, file_path, file_row_number_clause
            ),
        };
        sql = match &schema.get_sample_clause() {
            Some(sample_clause) => format!("{} {}", sql, sample_clause),
            None => sql,
        };

        let config = Config::default().threads(get_elephantduck_threads().into()).unwrap();
        let connection = Connection::open_in_memory_with_flags(config).unwrap();

        let statement = unsafe {
            let statement = Box::leak(Box::new(connection.prepare(&sql).unwrap()));
            std::mem::transmute::<&mut Statement<'_>, &mut Statement<'static>>(statement)
        };

        let arrow_stream = unsafe {
            let arrow_stream = Box::leak(Box::new(statement.stream_arrow([], Arc::new(arrow_schema)).unwrap()));
            std::mem::transmute::<&mut ArrowStream<'_>, &mut ArrowStream<'static>>(arrow_stream)
        };

        Self {
            statement,
            arrow_stream,
            record_batch: None,
            pg_types: Some(schema.get_pg_types()),
            current_row: 0,
        }
    }

    pub fn read(&mut self, row: &mut TupleSlot) -> bool {
        match &mut self.record_batch {
            Some(record_batch) => {
                if self.current_row >= record_batch.num_rows() {
                    self.record_batch = self.arrow_stream.next();
                    self.current_row = 0;
                }
            }
            None => {
                self.record_batch = self.arrow_stream.next();
                self.current_row = 0;
            }
        }

        match &self.record_batch {
            Some(record_batch) => {
                let pg_types = self.pg_types.as_ref().unwrap();
                for (column_index, pg_type) in pg_types.iter().enumerate() {
                    let field = record_batch.column(column_index);
                    convert_datum_arrow_to_pg(field, column_index, *pg_type, self.current_row, row);
                }
                self.current_row += 1;
                true
            }
            None => false,
        }
    }

    pub fn close(&mut self) {
        unsafe {
            let _ = Box::from_raw(self.arrow_stream);
            let _ = Box::from_raw(self.statement);
        }
    }
}
