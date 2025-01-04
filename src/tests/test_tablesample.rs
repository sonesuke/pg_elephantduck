// test for table access method(TAM) interface

#[allow(unused_imports)]
use pgrx::prelude::*;

#[pg_schema]
#[cfg(any(test, feature = "pg_test"))]
pub mod tests {

    use super::*;
    use crate::tests::utils::tests::pg_test_setup;

    #[pg_test]
    fn test_tablesample_clause() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test USING elephantduck AS SELECT GENERATE_SERIES(1, 100) AS num;",
        );

        let count = Spi::get_one::<i64>("SELECT COUNT(*)::INT8 FROM test TABLESAMPLE SYSTEM (10) REPEATABLE (0);");
        assert_ne!(count, Ok(Some(15)), "should be around 10");
        assert_ne!(Ok(Some(5)), count, "should be around 10");
    }
}
