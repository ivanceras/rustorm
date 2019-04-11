/// Copied from diesel
///
/// Intervals in Postgres are separated into 3 parts. A 64 bit integer representing time in
/// microseconds, a 32 bit integer representing number of days, and a 32 bit integer
/// representing number of months. This struct is a dumb wrapper type, meant only to indicate the
/// meaning of these parts.
///
use byteorder::{BigEndian, ReadBytesExt};
use postgres::types::{self, FromSql, Type};
use std::error::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgInterval {
    /// The number of whole microseconds
    pub microseconds: i64,
    /// The number of whole days
    pub days: i32,
    /// The number of whole months
    pub months: i32,
}

impl PgInterval {
    /// Constructs a new `PgInterval`
    ///
    /// No conversion occurs on the arguments. It is valid to provide a number
    /// of microseconds greater than the longest possible day, or a number of
    /// days greater than the longest possible month, as it is impossible to say
    /// how many months are in "40 days" without knowing a precise date.
    pub fn new(microseconds: i64, days: i32, months: i32) -> Self {
        PgInterval {
            microseconds: microseconds,
            days: days,
            months: months,
        }
    }

    /// Equivalent to `new(microseconds, 0, 0)`
    pub fn from_microseconds(microseconds: i64) -> Self {
        Self::new(microseconds, 0, 0)
    }

    /// Equivalent to `new(0, days, 0)`
    pub fn from_days(days: i32) -> Self {
        Self::new(0, days, 0)
    }

    /// Equivalent to `new(0, 0, months)`
    pub fn from_months(months: i32) -> Self {
        Self::new(0, 0, months)
    }

    /*
    /// rough microseconds
    /// 1 day   =    86_400_000_000 ms
    /// 1 month = 2_629_800_000_000 ms
    /// by duckduckgo
    pub fn microseconds(&self) -> i64 {
        self.months * 2_629_800_000_000i64 + self.days * 86_400_000_000i64 + self.microseconds
    }
    */
}

impl FromSql for PgInterval {
    fn from_sql(_ty: &Type, bytes: &[u8]) -> Result<Self, Box<Error + Send + Sync>> {
        let mut bytes = bytes.clone();
        let ms = bytes.read_i64::<BigEndian>()?;
        let days = bytes.read_i32::<BigEndian>()?;
        let months = bytes.read_i32::<BigEndian>()?;
        Ok(PgInterval::new(ms, days, months))
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            types::INTERVAL => true,
            _ => false,
        }
    }
}
