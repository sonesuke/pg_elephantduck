// test for table access method(TAM) interface

#[allow(unused_imports)]
use pgrx::prelude::*;

#[pg_schema]
#[cfg(any(test, feature = "pg_test"))]
pub mod tests {
    use crate::tests::utils::tests::pg_test_setup;

    use super::*;

    #[pg_test]
    fn test_where_clause() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test USING elephantduck AS SELECT GENERATE_SERIES(1, 10) AS num;",
        );

        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*)::INT8 FROM test WHERE num < 5;"),
            Ok(Some(4)),
            "should be 4"
        );
    }

    #[pg_test]
    fn test_where_clause_that_have_vars_not_in_selected_columns() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test (
                a INTEGER,
                b INTEGER
            ) USING elephantduck;
            INSERT INTO test VALUES (1, 2);",
        );

        assert_eq!(
            Spi::get_one::<i64>("SELECT a FROM test WHERE b IS NOT NULL;"),
            Ok(Some(1)),
            "should be 1"
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT b FROM test WHERE a IS NOT NULL;"),
            Ok(Some(2)),
            "should be 2"
        );
    }

    #[pg_test]
    fn test_where_clause_with_some_expression() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test USING elephantduck AS SELECT GENERATE_SERIES(1, 10) AS num;",
        );

        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*)::INT8 FROM test WHERE num + 1 < 5 - 2;"),
            Ok(Some(1)),
            "should be 1"
        );
    }

    #[pg_test]
    fn test_where_clause_with_some_function() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test USING elephantduck AS SELECT GENERATE_SERIES(1, 10) AS num;",
        );

        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*)::INT8 FROM test WHERE RANDOM() IS NOT NULL;"),
            Ok(Some(10)),
            "should be 10"
        );
    }
}
