use crate::common::*;
use crate::datatype_util::*;
use arrow::datatypes::Schema as ArrowSchema;
use parquet::file::properties::WriterProperties;

use std::sync::Arc;

pub struct ArrowWriter {
    writer: Option<parquet::arrow::arrow_writer::ArrowWriter<std::fs::File>>,
    schema: Schema,
    arrow_schema: ArrowSchema,
}

impl ArrowWriter {
    pub fn new(schema: &Schema) -> Self {
        let file_path = schema.get_path();
        let arrow_schema = schema.get_arrow_schema();
        let parquet_file = std::fs::File::create(file_path.clone()).unwrap();
        let writer_properties = WriterProperties::builder()
            .set_compression(parquet::basic::Compression::ZSTD(
                parquet::basic::ZstdLevel::try_new(3).unwrap(),
            ))
            .build();

        Self {
            writer: Some(
                parquet::arrow::arrow_writer::ArrowWriter::try_new(
                    parquet_file,
                    Arc::new(arrow_schema.clone()),
                    Some(writer_properties),
                )
                .unwrap(),
            ),
            schema: schema.clone(),
            arrow_schema: arrow_schema,
        }
    }

    pub fn write(&mut self, row: &TupleSlot) {
        if let Some(writer) = &mut self.writer {
            let pg_types = self.schema.get_pg_types();
            let record_batch = arrow::record_batch::RecordBatch::try_new(
                Arc::new(self.arrow_schema.clone()),
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

    pub fn close(&mut self) {
        if let Some(writer) = self.writer.take() {
            writer.close().unwrap();
        }
    }
}
