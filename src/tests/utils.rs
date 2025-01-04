// test for table access method(TAM) interface

#[allow(unused_imports)]
use pgrx::prelude::*;

#[pg_schema]
#[cfg(any(test, feature = "pg_test"))]
pub mod tests {
    use super::*;

    pub fn pg_test_setup() {
        let _ = Spi::run(
            "
        DROP EXTENSION IF EXISTS pg_elephantduck CASCADE;
        CREATE EXTENSION pg_elephantduck;
        ",
        );
    }
}
