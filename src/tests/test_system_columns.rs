// test for table access method(TAM) interface

#[allow(unused_imports)]
use pgrx::prelude::*;

#[pg_schema]
#[cfg(any(test, feature = "pg_test"))]
pub mod tests {
    use super::*;
    use crate::tests::utils::tests::pg_test_setup;

    #[pg_test]
    fn test_system_column() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test USING elephantduck AS SELECT GENERATE_SERIES(1, 10) AS num;",
        );

        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(ctid) FROM test;"),
            Ok(Some(10)),
            "should be 10"
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(ctid) FROM test WHERE num < 5;"),
            Ok(Some(4)),
            "should be 4"
        );
    }
}
