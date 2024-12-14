use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{Field, Fields, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::file::properties::WriterProperties;

use duckdb::{ArrowStream, Config, Connection, Statement};

use pgrx::pg_sys::{self};
use pgrx::prelude::*;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

pub struct Attribute {
    pub column_id: u32,
    pub data_type: pg_sys::Oid,
}

pub type Schema = Vec<Attribute>;

#[derive(Clone)]
pub struct Value {
    pub datum: pg_sys::Datum,
    pub is_null: bool,
}

pub type Row = Vec<Value>;

struct DuckdbReader {
    statement: &'static mut Statement<'static>,
    arrow_stream: &'static mut ArrowStream<'static>,
    record_batch: Option<RecordBatch>,
    current_row: usize,
}

impl DuckdbReader {
    pub fn new(sql: String, schema: SchemaRef) -> Self {
        let config = Config::default().threads(16).unwrap();
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

    pub fn read(&mut self) -> Option<Row> {
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
                let row = record_batch
                    .columns()
                    .iter()
                    .map(|column| convert_datum_arrow_to_pg(column, self.current_row))
                    .collect();
                self.current_row += 1;
                Some(row)
            }
            None => None,
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

    pub fn write(&mut self, row: Row) {
        if self.writer.is_none() {
            let file_path = format!("table_{}.parquet", self.table_id);

            let parquet_file = std::fs::File::create(file_path.clone()).unwrap();
            debug1!("file_path: {}", file_path);

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
                row.iter()
                    .zip(self.pg_types.as_ref().unwrap().iter())
                    .map(|(v, t)| convert_datum_pg_to_arrow(*t, v.datum, v.is_null))
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

    pub fn read(&mut self) -> Option<Row> {
        if self.reader.is_none() {
            let file_path = format!("table_{}.parquet", self.table_id);
            self.reader = Some(DuckdbReader::new(
                format!("SELECT * FROM parquet_scan('{}')", file_path),
                Arc::new(self.schema.clone().unwrap()),
            ));
        }

        match &mut self.reader {
            Some(reader) => reader.read(),
            None => {
                debug1!("Reader is None");
                None
            }
        }
    }

    pub fn close(&mut self) {
        if let Some(writer) = self.writer.take() {
            writer.close().unwrap();
            debug1!("Writer closed");
        }
        if let Some(mut reader) = self.reader.take() {
            reader.close();
            debug1!("Reader closed");
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

fn convert_datum_arrow_to_pg(field: &ArrayRef, current_row: usize) -> Value {
    match field.data_type() {
        arrow::datatypes::DataType::Boolean => {
            let array = field.as_any().downcast_ref::<arrow::array::BooleanArray>().unwrap();
            Value {
                datum: array.value(current_row).into_datum().unwrap(),
                is_null: array.is_null(current_row),
            }
        }
        arrow::datatypes::DataType::Int32 => {
            let array = field.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
            Value {
                datum: array.value(current_row).into_datum().unwrap(),
                is_null: array.is_null(current_row),
            }
        }
        arrow::datatypes::DataType::Int64 => {
            let array = field.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap();
            Value {
                datum: array.value(current_row).into_datum().unwrap(),
                is_null: array.is_null(current_row),
            }
        }
        arrow::datatypes::DataType::Float32 => {
            let array = field.as_any().downcast_ref::<arrow::array::Float32Array>().unwrap();
            Value {
                datum: array.value(current_row).into_datum().unwrap(),
                is_null: array.is_null(current_row),
            }
        }
        arrow::datatypes::DataType::Float64 => {
            let array = field.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap();
            Value {
                datum: array.value(current_row).into_datum().unwrap(),
                is_null: array.is_null(current_row),
            }
        }
        arrow::datatypes::DataType::Utf8 => {
            let array = field.as_any().downcast_ref::<arrow::array::StringArray>().unwrap();
            Value {
                datum: array.value(current_row).into_datum().unwrap(),
                is_null: array.is_null(current_row),
            }
        }
        _ => panic!("Invalid data type {:?}", field.data_type()),
    }
}

pub fn create_table(table_id: u32, schema: Box<Schema>) {
    unsafe {
        match VIRTUAL_STORAGE.lock() {
            Ok(mut storage) => {
                let mut table = Table::new(table_id);
                table.set_schema(*schema);
                storage.insert(table_id, table);
            }
            Err(_) => {
                debug1!("Failed to lock storage")
            }
        }
    }
}

pub fn insert_table(table_id: u32, row: Row) {
    unsafe {
        match VIRTUAL_STORAGE.lock() {
            Ok(mut storage) => match storage.get_mut(&table_id) {
                Some(table) => {
                    table.write(row);
                }
                None => {
                    debug1!("Table not found");
                }
            },
            Err(_) => {
                debug1!("Failed to lock storage")
            }
        }
    }
}

pub fn close_tables() {
    unsafe {
        match VIRTUAL_STORAGE.lock() {
            Ok(mut storage) => {
                for table in storage.values_mut() {
                    (*table).close();
                }
            }
            Err(_) => {
                debug1!("Failed to lock storage")
            }
        }
    }
}

pub fn set_schema_for_read(table_id: u32, schema: Box<Schema>) {
    unsafe {
        match VIRTUAL_STORAGE.lock() {
            Ok(mut storage) => match storage.get_mut(&table_id) {
                Some(table) => {
                    table.set_schema(*schema);
                }
                None => {
                    info!("Table not found, then create table");
                    let mut table = Table::new(table_id);
                    table.set_schema(*schema);
                    storage.insert(table_id, table);
                }
            },
            Err(_) => {
                info!("Failed to lock storage")
            }
        }
    }
}

pub fn get_row(table_id: u32) -> Option<Row> {
    unsafe {
        match VIRTUAL_STORAGE.lock() {
            Ok(mut storage) => match storage.get_mut(&table_id) {
                Some(table) => table.read(),
                None => {
                    debug1!("Table not found");
                    None
                }
            },
            Err(_) => {
                debug1!("Failed to lock storage");
                None
            }
        }
    }
}
