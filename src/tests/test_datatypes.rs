// test for table access method(TAM) interface

#[allow(unused_imports)]
use pgrx::prelude::*;

#[pg_schema]
#[cfg(any(test, feature = "pg_test"))]
pub mod tests {
    use super::*;
    use crate::tests::utils::tests::pg_test_setup;
    use std::str::FromStr;

    #[pg_test]
    fn test_create_table_various_integer_types() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test (integer INTEGER, bigint BIGINT) USING elephantduck;
             INSERT INTO test VALUES (1, 10);",
        );

        assert_eq!(
            Spi::get_one::<i32>("SELECT integer FROM test;"),
            Ok(Some(1)),
            "should be 1"
        );
        assert_eq!(
            Spi::get_one::<i64>("SELECT bigint FROM test;"),
            Ok(Some(10)),
            "should be 10"
        );
    }

    #[pg_test]
    fn test_create_table_various_float_types() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test (real REAL, double DOUBLE PRECISION) USING elephantduck;
             INSERT INTO test VALUES (1.0, 10.0);",
        );

        assert_eq!(
            Spi::get_one::<f32>("SELECT real FROM test;"),
            Ok(Some(1.0)),
            "should be 1.0"
        );
        assert_eq!(
            Spi::get_one::<f64>("SELECT double FROM test;"),
            Ok(Some(10.0)),
            "should be 10.0"
        );
    }

    #[pg_test]
    fn test_create_table_various_bool_type() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test (bool BOOL) USING elephantduck;
             INSERT INTO test VALUES (true);",
        );

        assert_eq!(
            Spi::get_one::<bool>("SELECT bool FROM test;"),
            Ok(Some(true)),
            "should be true"
        );
    }

    #[pg_test]
    fn test_create_table_various_string_type() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test (text TEXT) USING elephantduck;
             INSERT INTO test VALUES ('Aa');",
        );

        assert_eq!(
            Spi::get_one::<&str>("SELECT text FROM test;"),
            Ok(Some("Aa")),
            "should be Aa"
        );
    }

    #[pg_test]
    fn test_create_table_various_datetime_types() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test (date DATE, time TIME, timestamp TIMESTAMP) USING elephantduck;
             INSERT INTO test VALUES ('2024-12-06'::DATE, '01:23:45'::TIME, '2024-12-06 01:23:45'::TIMESTAMP);",
        );

        assert_eq!(
            Spi::get_one::<Date>("SELECT date FROM test;"),
            Ok(Some(Date::from_str("2024-12-06").unwrap())),
            "should be '2024-12-06'"
        );
        assert_eq!(
            Spi::get_one::<Time>("SELECT time FROM test;"),
            Ok(Some(Time::from_str("01:23:45").unwrap())),
            "should be '01:23:45'"
        );
        assert_eq!(
            Spi::get_one::<Timestamp>("SELECT timestamp FROM test;"),
            Ok(Some(Timestamp::from_str("2024-12-06 01:23:45").unwrap())),
            "should be '2024-12-06 01:23:45'"
        );
    }

    #[pg_test]
    fn test_create_table_various_datetime_with_timezone_types() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test (time TIMETZ, timestamp TIMESTAMPTZ) USING elephantduck;
            INSERT INTO test VALUES ('01:23:45+0900'::TIMETZ, '2024-12-06 01:23:45+0900'::TIMESTAMPTZ);",
        );

        assert_eq!(
            Spi::get_one::<TimeWithTimeZone>("SELECT time FROM test;"),
            Ok(Some(TimeWithTimeZone::from_str("01:23:45+0900").unwrap())),
            "should be '01:23:45+0900'"
        );
        assert_eq!(
            Spi::get_one::<TimestampWithTimeZone>("SELECT timestamp FROM test;"),
            Ok(Some(
                TimestampWithTimeZone::from_str("2024-12-06 01:23:45+0900").unwrap()
            )),
            "should be '2024-12-06 01:23:45+0900'"
        );
    }
}
