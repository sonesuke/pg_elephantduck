// test for table access method(TAM) interface

#[allow(unused_imports)]
use pgrx::prelude::*;

#[pg_schema]
#[cfg(any(test, feature = "pg_test"))]
pub mod tests {
    use std::str::FromStr;

    use super::*;

    fn pg_test_setup() {
        let _ = Spi::run(
            "
        DROP EXTENSION IF EXISTS pg_elephantduck CASCADE;
        CREATE EXTENSION pg_elephantduck;
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
        let _ = Spi::run(
            "
        DROP TABLE IF EXISTS test CASCADE;
        CREATE TABLE test (num INT) USING elephantduck;
        ",
        );
    }

    #[pg_test]
    fn test_insert_one() {
        pg_test_setup();

        let _ = Spi::run(
            "
        DROP TABLE IF EXISTS test CASCADE;
        CREATE TABLE test (num INT) USING elephantduck;
        INSERT INTO test VALUES (3);
        ",
        );
        let result = Spi::get_one::<i32>("SELECT num FROM test;");
        assert_eq!(result, Ok(Some(3)), "Should be 3 as inserted");
    }

    #[pg_test]
    fn test_insert_two() {
        pg_test_setup();

        let _ = Spi::run(
            "
        DROP TABLE IF EXISTS test CASCADE;
        CREATE TABLE test (num INT) USING elephantduck;
        INSERT INTO test VALUES (1), (2);
        ",
        );
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

    #[pg_test]
    fn test_create_table_various_integer_fields() {
        pg_test_setup();

        let _ = Spi::run(
            "
        DROP TABLE IF EXISTS test;
        CREATE TABLE test (
            integer INTEGER,
            bigint BIGINT
        ) USING elephantduck;
        INSERT INTO test VALUES (1, 10);
        ",
        );

        let result_int = Spi::get_one::<i32>("SELECT integer FROM test;");
        assert_eq!(result_int, Ok(Some(1)), "Count should be 1");

        let result_bigint = Spi::get_one::<i64>("SELECT bigint FROM test;");
        assert_eq!(result_bigint, Ok(Some(10)), "Count should be 10");
    }

    #[pg_test]
    fn test_create_table_various_float_fields() {
        pg_test_setup();

        let _ = Spi::run(
            "
        DROP TABLE IF EXISTS test;
        CREATE TABLE test (
            real REAL,
            double DOUBLE PRECISION
        ) USING elephantduck;
        INSERT INTO test VALUES (1.0, 10.0);
        ",
        );

        let result_num = Spi::get_one::<f32>("SELECT real FROM test;");
        assert_eq!(result_num, Ok(Some(1.0)), "Count should be 1.0");

        let result_float = Spi::get_one::<f64>("SELECT double FROM test;");
        assert_eq!(result_float, Ok(Some(10.0)), "Count should be 10.0");
    }

    #[pg_test]
    fn test_create_table_various_bool_fields() {
        pg_test_setup();

        let _ = Spi::run(
            "
        DROP TABLE IF EXISTS test;
        CREATE TABLE test (
            bool BOOL
        ) USING elephantduck;
        INSERT INTO test VALUES (true);
        ",
        );

        let result_num = Spi::get_one::<bool>("SELECT bool FROM test;");
        assert_eq!(result_num, Ok(Some(true)), "Count should be true");
    }

    #[pg_test]
    fn test_create_table_various_string_fields() {
        pg_test_setup();

        let _ = Spi::run(
            "
        DROP TABLE IF EXISTS test;
        CREATE TABLE test (
            text TEXT
        ) USING elephantduck;
        INSERT INTO test VALUES ('Aa');
        ",
        );
        let result_text = Spi::get_one::<&str>("SELECT text FROM test;");
        assert_eq!(result_text, Ok(Some("Aa")), "Count should be Aa");
    }

    #[pg_test]
    fn test_create_table_various_datetime_fields() {
        pg_test_setup();

        let _ = Spi::run(
            "
        DROP TABLE IF EXISTS test;
        CREATE TABLE test (
            date DATE,
            time TIME,
            timestamp TIMESTAMP
        ) USING elephantduck;
        INSERT INTO test VALUES ('2024-12-06'::DATE, '01:23:45'::TIME, '2024-12-06 01:23:45'::TIMESTAMP);
        ",
        );

        let result_date = Spi::get_one::<Date>("SELECT date FROM test;");
        assert_eq!(
            result_date,
            Ok(Some(Date::from_str("2024-12-06").unwrap())),
            "It should be '2024-12-06'"
        );

        let result_timestamp = Spi::get_one::<Timestamp>("SELECT timestamp FROM test;");
        assert_eq!(
            result_timestamp,
            Ok(Some(Timestamp::from_str("2024-12-06 01:23:45").unwrap())),
            "Count should be '2024-12-06 01:23:45"
        );
    }

    #[pg_test]
    fn test_push_down_where_clause() {
        pg_test_setup();

        let _ = Spi::run(
            "
        DROP TABLE IF EXISTS test;
        CREATE TABLE test USING elephantduck AS SELECT GENERATE_SERIES(1, 10) AS num;
        ",
        );
        let count = Spi::get_one::<i64>("SELECT COUNT(*)::INT8 FROM test WHERE num < 5;");
        assert_eq!(count, Ok(Some(4)), "Should generate 4 rows");
    }

    #[pg_test]
    fn test_where_clause() {
        pg_test_setup();

        let _ = Spi::run(
            "
        DROP TABLE IF EXISTS test;
        CREATE TABLE test (
            a INTEGER,
            b INTEGER
        ) USING elephantduck;
        INSERT INTO test VALUES (1, 2);
        ",
        );

        let result_a = Spi::get_one::<i64>("SELECT a FROM test WHERE b IS NOT NULL;");
        assert_eq!(result_a, Ok(Some(1)), "Count should be 1");

        let result_b = Spi::get_one::<i64>("SELECT b FROM test WHERE a IS NOT NULL;");
        assert_eq!(result_b, Ok(Some(2)), "Count should be 2");
    }

    #[pg_test]
    fn test_tablesample_clause() {
        pg_test_setup();

        let _ = Spi::run(
            "
        DROP TABLE IF EXISTS test;
        CREATE TABLE test USING elephantduck AS SELECT GENERATE_SERIES(1, 100) AS num;
        ",
        );
        let count = Spi::get_one::<i64>("SELECT COUNT(*)::INT8 FROM test TABLESAMPLE SYSTEM (10) REPEATABLE (0);");
        assert_ne!(count, Ok(Some(15)), "Should generate around 10 rows");
        assert_ne!(Ok(Some(5)), count, "Should generate around 10 rows");
    }

    #[pg_test]
    fn test_ctid() {
        pg_test_setup();

        let _ = Spi::run(
            "
        DROP TABLE IF EXISTS test;
        CREATE TABLE test USING elephantduck AS SELECT GENERATE_SERIES(1, 10) AS num;
        ",
        );
        let count = Spi::get_one::<i64>("SELECT COUNT(ctid) FROM test;");
        assert_eq!(count, Ok(Some(10)), "Should generate 10 rows");

        let count = Spi::get_one::<i64>("SELECT COUNT(ctid) FROM test WHERE num < 5;");
        assert_eq!(count, Ok(Some(4)), "Should generate 5 rows");
    }
}
