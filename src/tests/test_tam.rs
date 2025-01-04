// test for table access method(TAM) interface

#[allow(unused_imports)]
use pgrx::prelude::*;

#[pg_schema]
#[cfg(any(test, feature = "pg_test"))]
pub mod tests {
    use super::*;
    use crate::tests::utils::tests::pg_test_setup;

    #[pg_test]
    fn test_success_absolutely() {
        pg_test_setup();

        assert_eq!(Spi::get_one::<i32>("SELECT 1;"), Ok(Some(1)), "should be 1");
    }

    #[pg_test]
    fn test_create_table() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test CASCADE;
             CREATE TABLE test (num INT) USING elephantduck;",
        );
    }

    #[pg_test]
    fn test_insert_one() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test CASCADE;
             CREATE TABLE test (num INT) USING elephantduck;
             INSERT INTO test VALUES (3);",
        );

        assert_eq!(Spi::get_one::<i32>("SELECT num FROM test;"), Ok(Some(3)), "should be 3");
    }

    #[pg_test]
    fn test_insert_two() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test CASCADE;
             CREATE TABLE test (num INT) USING elephantduck;
             INSERT INTO test VALUES (1), (2);",
        );

        assert_eq!(
            Spi::get_one::<i32>("SELECT COUNT(*)::INT FROM test;"),
            Ok(Some(2)),
            "should be 2"
        );
        assert_eq!(
            Spi::get_one::<i32>("SELECT MAX(num) FROM test;"),
            Ok(Some(2)),
            "should be 2"
        );
    }

    #[pg_test]
    fn test_create_table_as() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test USING elephantduck AS SELECT GENERATE_SERIES(1, 10) AS num;",
        );

        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*)::INT8 FROM test;"),
            Ok(Some(10)),
            "should be 10"
        );
    }

    #[pg_test]
    fn test_alter_table_access_method() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test AS SELECT GENERATE_SERIES(1, 10) AS num;
             ALTER TABLE test SET ACCESS METHOD elephantduck;",
        );

        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*)::INT8 FROM test;"),
            Ok(Some(10)),
            "should be 10"
        );
    }

    #[pg_test]
    fn test_insert_multiple_times() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test (num INTEGER) USING elephantduck;
             INSERT INTO test VALUES (1);
             INSERT INTO test VALUES (2);",
        );

        assert_eq!(
            Spi::get_one::<i64>("SELECT COUNT(*)::INT8 FROM test;"),
            Ok(Some(2)),
            "should be 2"
        );
    }
}
