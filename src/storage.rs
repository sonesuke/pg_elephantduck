use pgrx::PgRelation;

use std::cell::OnceCell;
use std::collections::HashMap;

use crate::common::*;
use crate::reader::*;
use crate::writer::*;

pub struct Table {
    schema: Schema,
    writer: Option<ArrowWriter>,
    reader: Option<DuckdbReader>,
}

impl Table {
    pub fn new(schema: &Schema) -> Self {
        Self {
            schema: schema.clone(),
            writer: None,
            reader: None,
        }
    }

    pub fn set_schema(&mut self, schema: &Schema) {
        self.schema = schema.clone();
    }

    pub fn write(&mut self, row: TupleSlot) {
        if self.writer.is_none() {
            self.writer = Some(ArrowWriter::new(&self.schema));
        }

        if let Some(writer) = &mut self.writer {
            writer.write(&row);
        }
    }

    pub fn read(&mut self, row: &mut TupleSlot) -> bool {
        if self.reader.is_none() {
            self.reader = Some(DuckdbReader::new(&self.schema));
        }

        match &mut self.reader {
            Some(reader) => reader.read(row),
            None => false,
        }
    }

    pub fn close(&mut self) {
        if let Some(mut writer) = self.writer.take() {
            writer.close();
        }
        if let Some(mut reader) = self.reader.take() {
            reader.close();
        }
    }

    pub fn drop(&mut self) {
        let file_path = self.schema.get_path();
        let _ = std::fs::remove_file(file_path);
    }
}

type Storage = HashMap<u32, Table>;
fn get_storage() -> &'static mut Storage {
    static mut VIRTUAL_STORAGE: OnceCell<Storage> = OnceCell::new();
    unsafe {
        VIRTUAL_STORAGE.get_or_init(|| Storage::new());
        VIRTUAL_STORAGE.get_mut().unwrap()
    }
}

pub fn create_table(schema: &Schema) {
    let table_id = schema.relation.oid().as_u32();
    let storage = get_storage();
    storage.insert(table_id, Table::new(schema));
}

pub fn insert_table(row: TupleSlot) {
    let table_id = row.relation.oid().as_u32();
    let storage = get_storage();
    if let Some(table) = storage.get_mut(&table_id) {
        table.write(row);
    }
}

pub fn close_tables() {
    let storage = get_storage();
    for table in storage.values_mut() {
        (*table).close();
    }
}

pub fn set_schema_for_read(schema: Schema) {
    let storage = get_storage();
    let table_id = schema.relation.oid().as_u32();
    match storage.get_mut(&table_id) {
        Some(table) => {
            table.set_schema(&schema);
        }
        None => {
            storage.insert(table_id, Table::new(&schema));
        }
    }
}

pub fn read(row: &mut TupleSlot) -> bool {
    let storage = get_storage();
    let table_id = row.relation.oid().as_u32();

    match storage.get_mut(&table_id) {
        Some(table) => table.read(row),
        None => false,
    }
}

pub fn drop_table(relation: PgRelation) {
    let storage = get_storage();
    let relid = relation.oid().as_u32();
    if let Some(mut table) = storage.remove(&relid) {
        table.close();
        table.drop();
    }
}
