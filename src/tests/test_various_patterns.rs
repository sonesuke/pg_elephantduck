// test for table access method(TAM) interface

#[allow(unused_imports)]
use pgrx::prelude::*;

#[pg_schema]
#[cfg(any(test, feature = "pg_test"))]
pub mod tests {
    use super::*;
    use crate::tests::utils::tests::pg_test_setup;

    #[pg_test]
    fn test_order_by() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test USING elephantduck AS SELECT GENERATE_SERIES(1, 10) AS num;",
        );

        assert_eq!(
            Spi::get_one::<i64>("SELECT num AS no FROM test ORDER BY num DESC LIMIT 1;"),
            Ok(Some(10)),
            "should be 10"
        );
    }

    #[pg_test]
    fn test_aggregation() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test USING elephantduck AS SELECT GENERATE_SERIES(1, 10) AS num;",
        );

        assert_eq!(
            Spi::get_one::<i64>("SELECT SUM(num) AS no FROM test;"),
            Ok(Some(55)),
            "should be 55"
        );
    }

    #[pg_test]
    fn test_select_distinct_with_cte() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test (num INTEGER) USING elephantduck;
             INSERT INTO test VALUES (1), (1), (2), (2), (3), (3), (4), (4), (5), (5);",
        );

        assert_eq!(
            Spi::get_one::<i64>(
                "WITH _1 AS (SELECT DISTINCT (num) AS no FROM test)
                 SELECT COUNT(*) FROM _1;"
            ),
            Ok(Some(5)),
            "should be 5"
        );
    }

    #[pg_test]
    fn test_window_with_cte() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test;
             CREATE TABLE test USING elephantduck AS SELECT GENERATE_SERIES(1, 10) AS num;",
        );

        assert_eq!(
            Spi::get_one::<pgrx::AnyNumeric>(
                "WITH _1 AS (SELECT SUM(num) OVER (ORDER BY num) AS cumsum FROM test)
                 SELECT SUM(cumsum) FROM _1;"
            ),
            Ok(Some(pgrx::AnyNumeric::from(220))),
            "should be 220"
        );
    }

    #[pg_test]
    fn test_join() {
        pg_test_setup();

        let _ = Spi::run(
            "DROP TABLE IF EXISTS test1;
             DROP TABLE IF EXISTS test2;
             CREATE TABLE test1 USING elephantduck AS SELECT GENERATE_SERIES(1, 10) AS num1;
             CREATE TABLE test2 USING elephantduck AS SELECT GENERATE_SERIES(1, 10) AS num2;",
        );

        assert_eq!(
            Spi::get_one::<i64>("SELECT num2 FROM test1 JOIN test2 ON num1+1 = num2 WHERE num1 = 3;",),
            Ok(Some(4)),
            "should be 4"
        );
    }
}
