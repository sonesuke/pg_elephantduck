use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, Timelike};

const fn midnight() -> NaiveTime {
    match NaiveTime::from_hms_opt(0, 0, 0) {
        Some(time) => time,
        None => panic!("Invalid time"),
    }
}

const fn epoch_day() -> NaiveDate {
    match NaiveDate::from_ymd_opt(1970, 1, 1) {
        Some(date) => date,
        None => panic!("Invalid date"),
    }
}

const fn epoch_time() -> NaiveDateTime {
    NaiveDateTime::new(epoch_day(), midnight())
}

const MIDNIGHT: NaiveTime = midnight();
const EPOCH_DAY: NaiveDate = epoch_day();
const EPOCH_TIME: NaiveDateTime = epoch_time();

pub trait EpochForTime {
    fn from_epoch_day(epoch_time: i32) -> Self;
    fn to_epoch_day(&self) -> i32;
}

impl EpochForTime for pgrx::datum::Date {
    fn from_epoch_day(epoch_day: i32) -> Self {
        let date = EPOCH_DAY + Duration::days(epoch_day as i64);

        let year = date.year();
        let month = date.month() as u8;
        let day = date.day() as u8;

        pgrx::datum::Date::new(year, month, day).unwrap()
    }

    /// Converts `TimeWithTimeZone` to epoch time (seconds since 1970-01-01 00:00:00 UTC)
    fn to_epoch_day(&self) -> i32 {
        self.to_unix_epoch_days()
    }
}

pub trait EpochTimeZone {
    fn from_epoch_time(epoch_time: i64) -> Self;
    fn to_epoch_time(&self) -> i64;
}

impl EpochTimeZone for pgrx::datum::Timestamp {
    fn from_epoch_time(epoch_time: i64) -> Self {
        let datetime = EPOCH_TIME + Duration::seconds(epoch_time);

        let year = datetime.year();
        let month = datetime.month() as u8;
        let day = datetime.day() as u8;
        let hour = datetime.hour() as u8;
        let minute = datetime.minute() as u8;
        let second = datetime.second() as f64;

        pgrx::datum::Timestamp::new(year, month, day, hour, minute, second).unwrap()
    }

    /// Converts `TimeWithTimeZone` to epoch time (seconds since 1970-01-01 00:00:00 UTC)
    fn to_epoch_time(&self) -> i64 {
        let year = self.year();
        let month = self.month();
        let day = self.day();
        let hour = self.hour();
        let minute = self.minute();
        let second = self.second();

        let datetime = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(year, month as u32, day as u32).unwrap(),
            NaiveTime::from_hms_opt(hour as u32, minute as u32, second as u32).unwrap(),
        );

        datetime.signed_duration_since(EPOCH_TIME).num_seconds()
    }
}

impl EpochTimeZone for pgrx::datum::Time {
    fn from_epoch_time(epoch_time: i64) -> Self {
        let time = MIDNIGHT + Duration::seconds(epoch_time);

        let hour = time.hour() as u8;
        let minute = time.minute() as u8;
        let second = time.second() as f64;

        pgrx::datum::Time::new(hour, minute, second).unwrap()
    }

    /// Converts `TimeWithTimeZone` to epoch time (seconds since 1970-01-01 00:00:00 UTC)
    fn to_epoch_time(&self) -> i64 {
        let hour = self.hour();
        let minute = self.minute();
        let second = self.second();

        let time = NaiveTime::from_hms_opt(hour as u32, minute as u32, second as u32).unwrap();
        time.signed_duration_since(MIDNIGHT).num_seconds()
    }
}
