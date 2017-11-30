use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use std::error::Error;
use std::io::prelude::*;

use postgres::types::{self,ToSql,FromSql,Type,IsNull};

use bigdecimal::BigDecimal;
use num_bigint::{BigInt, BigUint, Sign};
use num_integer::Integer;
use num_traits::{Signed, ToPrimitive, Zero};


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgNumeric {
    Positive {
        weight: i16,
        scale: u16,
        digits: Vec<i16>,
    },
    Negative {
        weight: i16,
        scale: u16,
        digits: Vec<i16>,
    },
    NaN,
}

#[derive(Debug, Clone, Copy)]
struct InvalidNumericSign(u16);

impl ::std::fmt::Display for InvalidNumericSign {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "InvalidNumericSign({0:x})", self.0)
    }
}

impl Error for InvalidNumericSign {
    fn description(&self) -> &str {
        "sign for numeric field was not one of 0, 0x4000, 0xC000"
    }
}

impl FromSql for PgNumeric {

    fn from_sql(ty: &Type, bytes: &[u8]) -> Result<Self, Box<Error + Send + Sync>> {
        let mut bytes = bytes.clone();
        let ndigits = try!(bytes.read_u16::<NetworkEndian>());
        let mut digits = Vec::with_capacity(ndigits as usize);
        let weight = try!(bytes.read_i16::<NetworkEndian>());
        let sign = try!(bytes.read_u16::<NetworkEndian>());
        let scale = try!(bytes.read_u16::<NetworkEndian>());
        for _ in 0..ndigits {
            digits.push(try!(bytes.read_i16::<NetworkEndian>()));
        }

        match sign {
            0 => Ok(PgNumeric::Positive {
                weight: weight,
                scale: scale,
                digits: digits,
            }),
            0x4000 => Ok(PgNumeric::Negative {
                weight: weight,
                scale: scale,
                digits: digits,
            }),
            0xC000 => Ok(PgNumeric::NaN),
            invalid => Err(Box::new(InvalidNumericSign(invalid))),
        }
    }

    fn accepts(ty: &Type) -> bool{
        match *ty {
            types::NUMERIC => true,
            _ => panic!("can not accept type {:?}", ty), 
        }
    }
}

impl ToSql for PgNumeric {

    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>>{
        let sign = match *self {
            PgNumeric::Positive { .. } => 0,
            PgNumeric::Negative { .. } => 0x4000,
            PgNumeric::NaN => 0xC000,
        };
        let empty_vec = Vec::new();
        let digits = match *self {
            PgNumeric::Positive { ref digits, .. } | PgNumeric::Negative { ref digits, .. } => {
                digits
            }
            PgNumeric::NaN => &empty_vec,
        };
        let weight = match *self {
            PgNumeric::Positive { weight, .. } | PgNumeric::Negative { weight, .. } => weight,
            PgNumeric::NaN => 0,
        };
        let scale = match *self {
            PgNumeric::Positive { scale, .. } | PgNumeric::Negative { scale, .. } => scale,
            PgNumeric::NaN => 0,
        };
        try!(out.write_u16::<NetworkEndian>(digits.len() as u16));
        try!(out.write_i16::<NetworkEndian>(weight));
        try!(out.write_u16::<NetworkEndian>(sign));
        try!(out.write_u16::<NetworkEndian>(scale));
        for digit in digits.iter() {
            try!(out.write_i16::<NetworkEndian>(*digit));
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool{
        match *ty {
            types::NUMERIC => true,
            _ => false,
        }
    }

    to_sql_checked!();
}


impl From<PgNumeric> for BigDecimal {

    fn from(numeric: PgNumeric) -> Self {
        let (sign, weight, _, digits) = match numeric {
            PgNumeric::Positive {
                weight,
                scale,
                digits,
            } => (Sign::Plus, weight, scale, digits),
            PgNumeric::Negative {
                weight,
                scale,
                digits,
            } => (Sign::Minus, weight, scale, digits),
            PgNumeric::NaN => panic!("NaN is not (yet) supported in BigDecimal"),
        };
        let mut result = BigUint::default();
        let count = digits.len() as i64;
        for digit in digits {
            result = result * BigUint::from(10_000u64);
            result = result + BigUint::from(digit as u64);
        }
        // First digit got factor 10_000^(digits.len() - 1), but should get 10_000^weight
        let correction_exp = 4 * (i64::from(weight) - count + 1);
        // FIXME: `scale` allows to drop some insignificant figures, which is currently unimplemented.
        // This means that e.g. PostgreSQL 0.01 will be interpreted as 0.0100
        let result = BigDecimal::new(BigInt::from_biguint(sign, result), -correction_exp);
        result
    }
}
