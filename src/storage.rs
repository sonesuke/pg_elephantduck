use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{Field, Fields, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::file::properties::WriterProperties;

use duckdb::{ArrowStream, Config, Connection, Statement};

use pgrx::pg_sys::{self};
use pgrx::prelude::*;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use crate::settings::{get_elephantduck_path, get_elephantduck_threads};

pub struct Attribute {
    pub column_id: u32,
    pub data_type: pg_sys::Oid,
}

pub type Schema = Vec<Attribute>;

pub struct TupleSlot<'a> {
    pub natts: usize,
    pub datum: &'a mut [pg_sys::Datum],
    pub nulls: &'a mut [bool],
}

struct DuckdbReader {
    statement: &'static mut Statement<'static>,
    arrow_stream: &'static mut ArrowStream<'static>,
    record_batch: Option<RecordBatch>,
    current_row: usize,
}

impl DuckdbReader {
    pub fn new(sql: String, schema: SchemaRef) -> Self {
        let config = Config::default().threads(get_elephantduck_threads().into()).unwrap();
        let connection = Connection::open_in_memory_with_flags(config).unwrap();

        let statement = unsafe {
            let statement = Box::leak(Box::new(connection.prepare(&sql).unwrap()));
            std::mem::transmute::<&mut Statement<'_>, &mut Statement<'static>>(statement)
        };

        let arrow_stream = unsafe {
            let arrow_stream = Box::leak(Box::new(statement.stream_arrow([], schema).unwrap()));
            std::mem::transmute::<&mut ArrowStream<'_>, &mut ArrowStream<'static>>(arrow_stream)
        };

        Self {
            statement,
            arrow_stream,
            record_batch: None,
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
                for column_index in 0..record_batch.num_columns() {
                    let field = record_batch.column(column_index);
                    convert_datum_arrow_to_pg(field, column_index, self.current_row, row);
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

pub struct Table {
    table_id: u32,
    pg_types: Option<Vec<pg_sys::Oid>>,
    schema: Option<ArrowSchema>,
    writer: Option<parquet::arrow::arrow_writer::ArrowWriter<std::fs::File>>,
    reader: Option<DuckdbReader>,
}

impl Table {
    pub fn new(table_id: u32) -> Self {
        Self {
            table_id,
            pg_types: None,
            schema: None,
            writer: None,
            reader: None,
        }
    }

    fn get_path(&self, table_id: u32) -> String {
        let dir = get_elephantduck_path().unwrap().to_str().unwrap();
        format!("{}table_{}.parquet", dir, table_id)
    }

    pub fn set_schema(&mut self, schema: Schema) {
        let fields: Fields = schema
            .iter()
            .map(|attr| {
                Field::new(
                    format!("column_{}", attr.column_id),
                    convert_datatype_pg_to_arrow(attr.data_type),
                    true,
                )
            })
            .collect();
        self.schema = Some(ArrowSchema::new(fields));
        self.pg_types = Some(schema.iter().map(|attr| attr.data_type).collect());
    }

    pub fn write(&mut self, row: TupleSlot) {
        if self.writer.is_none() {
            let file_path = self.get_path(self.table_id);
            let parquet_file = std::fs::File::create(file_path.clone()).unwrap();
            let writer_properties = WriterProperties::builder()
                .set_compression(parquet::basic::Compression::ZSTD(
                    parquet::basic::ZstdLevel::try_new(3).unwrap(),
                ))
                .build();

            self.writer = Some(
                parquet::arrow::arrow_writer::ArrowWriter::try_new(
                    parquet_file,
                    Arc::new(self.schema.clone().unwrap()),
                    Some(writer_properties),
                )
                .unwrap(),
            );
        }

        if let Some(writer) = &mut self.writer {
            let record_batch = arrow::record_batch::RecordBatch::try_new(
                Arc::new(self.schema.clone().unwrap()),
                (0..row.natts)
                    .map(|i| convert_datum_pg_to_arrow(self.pg_types.as_ref().unwrap()[i], row.datum[i], row.nulls[i]))
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

    pub fn get_columns_clause(&self) -> String {
        if let Some(schema) = self.schema.as_ref() {
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<&str>>()
                .join(", ")
        } else {
            "1".to_string()
        }
    }

    pub fn read(&mut self, row: &mut TupleSlot) -> bool {
        if self.reader.is_none() {
            let file_path = self.get_path(self.table_id);
            let columns_clause = self.get_columns_clause();
            let sql = format!("SELECT {} FROM parquet_scan('{}')", columns_clause, file_path);
            self.reader = Some(DuckdbReader::new(sql, Arc::new(self.schema.clone().unwrap())));
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
        self.writer = None;
        self.reader = None;
    }
}

static mut VIRTUAL_STORAGE: LazyLock<Mutex<HashMap<u32, Table>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

fn convert_datatype_pg_to_arrow(data_type_oid: pg_sys::Oid) -> arrow::datatypes::DataType {
    match data_type_oid {
        pg_sys::BOOLOID => arrow::datatypes::DataType::Boolean,
        pg_sys::INT4OID => arrow::datatypes::DataType::Int32,
        pg_sys::INT8OID => arrow::datatypes::DataType::Int64,
        pg_sys::FLOAT4OID => arrow::datatypes::DataType::Float32,
        pg_sys::FLOAT8OID => arrow::datatypes::DataType::Float64,
        pg_sys::TEXTOID => arrow::datatypes::DataType::Utf8,
        _ => panic!("Invalid data type {:?}", data_type_oid),
    }
}

fn convert_datum_pg_to_arrow(
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
            pg_sys::TEXTOID => Arc::new(arrow::array::StringArray::from(vec![String::from_datum(
                datum, is_null,
            )])) as ArrayRef,
            _ => panic!("Invalid data type {:?}", data_type_oid),
        }
    }
}

fn convert_datum_arrow_to_pg(field: &ArrayRef, column_index: usize, current_row: usize, row: &mut TupleSlot) {
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
        arrow::datatypes::DataType::Int64 => {
            let array = field.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
            row.datum[column_index] = array.value(current_row).into_datum().unwrap();
            row.nulls[column_index] = array.is_null(current_row);
        }
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
        arrow::datatypes::DataType::Utf8 => {
            let array = field.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            row.datum[column_index] = array.value(current_row).into_datum().unwrap();
            row.nulls[column_index] = array.is_null(current_row);
        }
        _ => panic!("Invalid data type {:?}", field.data_type()),
    }
}

pub fn create_table(table_id: u32, schema: Schema) {
    unsafe {
        if let Ok(mut storage) = VIRTUAL_STORAGE.lock() {
            let mut table = Table::new(table_id);
            table.set_schema(schema);
            storage.insert(table_id, table);
        }
    }
}

pub fn insert_table(table_id: u32, row: TupleSlot) {
    unsafe {
        if let Ok(mut storage) = VIRTUAL_STORAGE.lock() {
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

pub fn set_schema_for_read(table_id: u32, schema: Schema) {
    unsafe {
        if let Ok(mut storage) = VIRTUAL_STORAGE.lock() {
            match storage.get_mut(&table_id) {
                Some(table) => {
                    table.set_schema(schema);
                }
                None => {
                    let mut table = Table::new(table_id);
                    table.set_schema(schema);
                    storage.insert(table_id, table);
                }
            }
        }
    }
}

pub fn read(table_id: u32, row: &mut TupleSlot) -> bool {
    unsafe {
        match VIRTUAL_STORAGE.lock() {
            Ok(mut storage) => match storage.get_mut(&table_id) {
                Some(table) => table.read(row),
                None => false,
            },
            Err(_) => false,
        }
    }
}
