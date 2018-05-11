///
/// Copied from diesel
///
use byteorder::{NetworkEndian, ReadBytesExt, WriteBytesExt};
use std::error::Error;

use postgres::types::{self, FromSql, IsNull, ToSql, Type};

use bigdecimal::BigDecimal;
use num_bigint::{BigInt, BigUint, Sign};

use num_integer::Integer;
use num_traits::Signed;
use num_traits::ToPrimitive;
use num_traits::Zero;

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
    fn from_sql(_ty: &Type, bytes: &[u8]) -> Result<Self, Box<Error + Send + Sync>> {
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

    fn accepts(ty: &Type) -> bool {
        match *ty {
            types::NUMERIC => true,
            _ => panic!("can not accept type {:?}", ty),
        }
    }
}

impl ToSql for PgNumeric {
    fn to_sql(&self, _ty: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
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

    fn accepts(ty: &Type) -> bool {
        match *ty {
            types::NUMERIC => true,
            _ => false,
        }
    }

    to_sql_checked!();
}

/// Iterator over the digits of a big uint in base 10k.
/// The digits will be returned in little endian order.
struct ToBase10000(Option<BigUint>);

impl Iterator for ToBase10000 {
    type Item = i16;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.take().map(|v| {
            let (div, rem) = v.div_rem(&BigUint::from(10_000u16));
            if !div.is_zero() {
                self.0 = Some(div);
            }
            rem.to_i16().expect("10000 always fits in an i16")
        })
    }
}

impl<'a> From<&'a BigDecimal> for PgNumeric {
    fn from(decimal: &'a BigDecimal) -> Self {
        let (mut integer, scale) = decimal.as_bigint_and_exponent();
        let scale = scale as u16;
        integer = integer.abs();

        // Ensure that the decimal will always lie on a digit boundary
        for _ in 0..(4 - scale % 4) {
            integer = integer * 10;
        }
        let integer = integer.to_biguint().expect("integer is always positive");

        let mut digits = ToBase10000(Some(integer)).collect::<Vec<_>>();
        digits.reverse();
        let digits_after_decimal = scale as u16 / 4 + 1;
        let weight = digits.len() as i16 - digits_after_decimal as i16 - 1;

        let unnecessary_zeroes = if weight >= 0 {
            let index_of_decimal = (weight + 1) as usize;
            digits
                .get(index_of_decimal..)
                .expect("enough digits exist")
                .iter()
                .rev()
                .take_while(|i| i.is_zero())
                .count()
        } else {
            0
        };

        let relevant_digits = digits.len() - unnecessary_zeroes;
        digits.truncate(relevant_digits);

        match decimal.sign() {
            Sign::Plus => PgNumeric::Positive {
                digits,
                scale,
                weight,
            },
            Sign::Minus => PgNumeric::Negative {
                digits,
                scale,
                weight,
            },
            Sign::NoSign => PgNumeric::Positive {
                digits: vec![0],
                scale: 0,
                weight: 0,
            },
        }
    }
}

impl From<BigDecimal> for PgNumeric {
    fn from(bigdecimal: BigDecimal) -> Self {
        (&bigdecimal).into()
    }
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
