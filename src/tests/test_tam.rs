// test for table access method(TAM) interface

#[allow(unused_imports)]
use pgrx::prelude::*;

#[pg_schema]
#[cfg(any(test, feature = "pg_test"))]
pub mod tests {
    use super::*;

    fn pg_test_setup() {
        let _ = Spi::run(
            "
        DROP EXTENSION IF EXISTS pg_elephantduck CASCADE;
        CREATE EXTENSION pg_elephantduck;
        ",
        );
    }

    fn create_test_table() {
        let _ = Spi::run(
            "
        DROP TABLE IF EXISTS test;
        CREATE TABLE test (num INT) USING elephantduck;
        ",
        );
    }

    #[pg_test]
    fn test_success_absolutely() {
        pg_test_setup();

        let result = Spi::get_one::<i32>("SELECT 1;");
        assert_eq!(result, Ok(Some(1)), "Should be 1 and success absolutely");
    }

    #[pg_test]
    fn test_create_table() {
        pg_test_setup();
        create_test_table();
    }

    #[pg_test]
    fn test_insert_one() {
        pg_test_setup();
        create_test_table();

        let _ = Spi::run("INSERT INTO test VALUES (3);");
        let result = Spi::get_one::<i32>("SELECT num FROM test;");
        assert_eq!(result, Ok(Some(3)), "Should be 3 as inserted");
    }

    #[pg_test]
    fn test_insert_two() {
        pg_test_setup();
        create_test_table();

        let _ = Spi::run("INSERT INTO test VALUES (1), (2);");
        let result_one = Spi::get_one::<i32>("SELECT COUNT(*)::INT FROM test;");
        assert_eq!(result_one, Ok(Some(2)), "Count should be 2");

        let result_two = Spi::get_one::<i32>("SELECT MAX(num) FROM test;");
        assert_eq!(result_two, Ok(Some(2)), "Max num hould be 2");
    }

    #[pg_test]
    fn test_create_table_as() {
        pg_test_setup();

        let _ = Spi::run(
            "
        DROP TABLE IF EXISTS test;
        CREATE TABLE test USING elephantduck AS SELECT GENERATE_SERIES(1, 10) AS num;
        ",
        );
        let count = Spi::get_one::<i64>("SELECT COUNT(*)::INT8 FROM test;");
        assert_eq!(count, Ok(Some(10)), "Should generate 10 rows");
    }
}
