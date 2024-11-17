use pgrx::prelude::*;

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

pub struct Attribute {
    pub column_id: u32,
    pub data_type: u32,
}

pub type Schema = Vec<Attribute>;

#[derive(Clone)]
pub struct Value {
    pub value: i32,
    pub is_null: bool,
}

pub type Row = Vec<Value>;

pub struct Table {
    schema: Schema,
    data: Vec<Row>,
}

static mut VIRTUAL_STORAGE: LazyLock<Mutex<HashMap<u32, Table>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

pub fn create_table(table_id: u32, schema: Schema) {
    unsafe {
        match VIRTUAL_STORAGE.lock() {
            Ok(mut storage) => {
                storage.insert(
                    table_id,
                    Table {
                        schema: schema,
                        data: Vec::new(),
                    },
                );
            }
            Err(_) => {
                info!("Failed to lock storage");
                panic!("Failed to lock storage");
            }
        }
    }
}

pub fn insert_table(table_id: u32, row: Row) {
    unsafe {
        match VIRTUAL_STORAGE.lock() {
            Ok(mut storage) => match storage.get_mut(&table_id) {
                Some(table) => {
                    table.data.push(row);
                }
                None => {
                    info!("Table not found");
                    panic!("Table not found");
                }
            },
            Err(_) => {
                info!("Failed to lock storage");
                panic!("Failed to lock storage");
            }
        }
    }
}

pub fn get_row<'a>(table_id: u32, index: usize) -> Option<Row> {
    unsafe {
        match VIRTUAL_STORAGE.lock() {
            Ok(storage) => match storage.get(&table_id) {
                Some(table) => table.data.get(index).cloned(),
                None => {
                    info!("Table not found");
                    None
                }
            },
            Err(_) => {
                info!("Failed to lock storage");
                None
            }
        }
    }
}
