use arrow::datatypes::{Field, Fields, Schema as ArrowSchema};
use parquet::file::properties::WriterProperties;

use pgrx::pg_sys::{self};
use pgrx::PgRelation;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use crate::common::*;
use crate::datatype_util::*;
use crate::reader::*;
use crate::writer::*;

pub struct Table {
    schema: Option<Schema>,
    arrow_schema: Option<ArrowSchema>,
    writer: Option<ArrowWriter>,
    reader: Option<DuckdbReader>,
}

impl Table {
    pub fn new() -> Self {
        Self {
            schema: None,
            arrow_schema: None,
            writer: None,
            reader: None,
        }
    }

    pub fn set_schema(&mut self, schema: Schema) {
        let fields: Fields = schema
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
        self.arrow_schema = Some(ArrowSchema::new(fields));
        self.schema = Some(schema);
    }

    pub fn write(&mut self, row: TupleSlot) {
        if self.writer.is_none() {
            let file_path = self.schema.as_mut().unwrap().get_path();
            let parquet_file = std::fs::File::create(file_path.clone()).unwrap();
            let writer_properties = WriterProperties::builder()
                .set_compression(parquet::basic::Compression::ZSTD(
                    parquet::basic::ZstdLevel::try_new(3).unwrap(),
                ))
                .build();

            self.writer = Some(
                parquet::arrow::arrow_writer::ArrowWriter::try_new(
                    parquet_file,
                    Arc::new(self.arrow_schema.clone().unwrap()),
                    Some(writer_properties),
                )
                .unwrap(),
            );
        }

        if let Some(writer) = &mut self.writer {
            let pg_types = self.schema.as_ref().unwrap().get_pg_types();
            let record_batch = arrow::record_batch::RecordBatch::try_new(
                Arc::new(self.arrow_schema.clone().unwrap()),
                (0..row.natts)
                    .map(|i| convert_datum_pg_to_arrow(pg_types[i], row.datum[i], row.nulls[i]))
                    .collect(),
            )
            .unwrap();
            match writer.write(&record_batch) {
                Ok(_) => {}
                Err(_) => {
                    panic!("Failed to write");
                }
            }
        }
    }

    pub fn read(&mut self, row: &mut TupleSlot) -> bool {
        if self.reader.is_none() {
            self.reader = Some(DuckdbReader::new(self.schema.as_ref().unwrap().clone()));
        }

        match &mut self.reader {
            Some(reader) => reader.read(row),
            None => false,
        }
    }

    pub fn close(&mut self) {
        if let Some(writer) = self.writer.take() {
            writer.close().unwrap();
        }
        if let Some(mut reader) = self.reader.take() {
            reader.close();
        }
    }

    pub fn drop(&mut self) {
        let file_path = self.schema.as_mut().unwrap().get_path();
        let _ = std::fs::remove_file(file_path);
    }
}

static mut VIRTUAL_STORAGE: LazyLock<Mutex<HashMap<u32, Table>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

pub fn create_table(schema: Schema) {
    unsafe {
        if let Ok(mut storage) = VIRTUAL_STORAGE.lock() {
            let table_id = schema.relation.oid().as_u32();
            let mut table = Table::new();
            table.set_schema(schema);
            storage.insert(table_id, table);
        }
    }
}

pub fn insert_table(row: TupleSlot) {
    unsafe {
        if let Ok(mut storage) = VIRTUAL_STORAGE.lock() {
            let table_id = row.relation.oid().as_u32();
            if let Some(table) = storage.get_mut(&table_id) {
                table.write(row);
            }
        }
    }
}

pub fn close_tables() {
    unsafe {
        if let Ok(mut storage) = VIRTUAL_STORAGE.lock() {
            for table in storage.values_mut() {
                (*table).close();
            }
        }
    }
}

pub fn set_schema_for_read(schema: Schema) {
    unsafe {
        if let Ok(mut storage) = VIRTUAL_STORAGE.lock() {
            let table_id = schema.relation.oid().as_u32();
            match storage.get_mut(&table_id) {
                Some(table) => {
                    table.set_schema(schema);
                }
                None => {
                    let mut table = Table::new();
                    table.set_schema(schema);
                    storage.insert(table_id, table);
                }
            }
        }
    }
}

pub fn read(row: &mut TupleSlot) -> bool {
    unsafe {
        let table_id = row.relation.oid().as_u32();
        match VIRTUAL_STORAGE.lock() {
            Ok(mut storage) => match storage.get_mut(&table_id) {
                Some(table) => table.read(row),
                None => false,
            },
            Err(_) => false,
        }
    }
}

pub fn drop_table(relation: PgRelation) {
    unsafe {
        if let Ok(mut storage) = VIRTUAL_STORAGE.lock() {
            let relid = relation.oid().as_u32();
            if let Some(mut table) = storage.remove(&relid) {
                table.close();
                table.drop();
            }
        }
    }
}
