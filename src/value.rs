use std::ops::{Add, BitOr};

use FieldKind;
use QueryError;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueKind {
    Boolean,
    Unsigned,
    Signed,
    Real,
    Text,
    Blob,
}
impl ValueKind {
    pub(crate) fn more_generic(&self, other: ValueKind)  -> Option<ValueKind> {
        if *self == other {
            Some(*self)
        }
        else {
            use self::ValueKind::*;
            match self {
                Unsigned => match other {
                    Signed => Some(Signed),
                    Real => Some(Real),
                    _ => None
                },
                Signed => match other {
                    Unsigned => Some(Signed),
                    Real => Some(Real),
                    _ => None
                },
                Real => match other {
                    Unsigned => Some(Real),
                    Signed => Some(Real),
                    _ => None
                },
                _ => None
            }
        }
    }
}

macro unwrap_binop($type:path, $v1:ident, $v2:ident, $op:path) {
    $type(
        $op(
            match $v1 {$type(b) => b, _ => unreachable!()},
            match $v2 {$type(b) => b, _ => unreachable!()}
        )
    )
}

/// Types for data processing
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// Boolean (true/false) value
    Boolean(bool),
    /// Unsigned integer
    Unsigned(u128),
    /// Signed integer
    Signed(i128),
    /// IEEE 754 double-precision binary floating-point format (binary64)
    Real(f64),
    /// UTF-8 Text
    Text(String),
    /// Arbitrary binary data
    Blob(Vec<u8>),
}
impl Value {
    pub fn kind(&self) -> ValueKind {
        use self::Value::*;
        match self {
            Boolean(_) => ValueKind::Boolean,
            Unsigned(_) => ValueKind::Unsigned,
            Signed(_) => ValueKind::Signed,
            Real(_) => ValueKind::Real,
            Text(_) => ValueKind::Text,
            Blob(_) => ValueKind::Blob,
        }
    }

    pub fn binop_add(&self, other: Value) -> Result<Value, QueryError> {
        if let Some(result_kind) = self.kind().more_generic(other.kind()) {
            let c1 = self.cast_to(result_kind)?;
            let c2 = other.cast_to(result_kind)?;

            use super::Value::*;
            match result_kind {
                // Value::Boolean(
                //     (match c1 {Value::Boolean(b) => b, _ => unreacable!()})
                //     ||
                //     (match c2 {Value::Boolean(b) => b, _ => unreacable!()})
                // )


                ValueKind::Boolean  => Ok(unwrap_binop!(Boolean,  c1, c2,   bool::bitor)),
                ValueKind::Unsigned => Ok(unwrap_binop!(Unsigned, c1, c2,   u128::saturating_add)),
                ValueKind::Signed   => Ok(unwrap_binop!(Signed,   c1, c2,   i128::saturating_add)),
                ValueKind::Real     => Ok(unwrap_binop!(Real,     c1, c2,    f64::add)),
                ValueKind::Text     => {
                    let v1 = match c1 {Value::Text(b) => b, _ => unreachable!()};
                    let v2 = match c2 {Value::Text(b) => b, _ => unreachable!()};
                    Ok(Value::Text(v1 + &v2))
                },
                ValueKind::Blob     => {
                    let v1 = match c1 {Value::Blob(b) => b, _ => unreachable!()};
                    let v2 = match c2 {Value::Blob(b) => b, _ => unreachable!()};
                    let mut v = v1.clone();
                    v.extend(v2.clone());
                    Ok(Value::Blob(v))
                },
            }
        }
        else {
            Err(QueryError::IncompatibleTypes)
        }
    }

    pub fn cast_to(&self, to: ValueKind) -> Result<Value, QueryError> {
        if self.kind() == to {
            return Ok(self.clone());
        }

        match to {
            ValueKind::Boolean => Err(QueryError::IncompatibleTypes),
            ValueKind::Unsigned => match self {
                Value::Signed(v)    => Ok(Value::Unsigned(*v as u128)),
                _ => Err(QueryError::IncompatibleTypes)
            },
            ValueKind::Signed => match self {
                Value::Unsigned(v)  => Ok(Value::Signed(*v as i128)),
                Value::Real(v)      => Ok(Value::Signed(*v as i128)),
                _ => Err(QueryError::IncompatibleTypes)
            },
            ValueKind::Real => match self {
                Value::Signed(v)    => Ok(Value::Real(*v as f64)),
                Value::Unsigned(v)  => Ok(Value::Real(*v as f64)),
                _ => Err(QueryError::IncompatibleTypes)
            },
            _ => Err(QueryError::IncompatibleTypes)
        }
    }

    pub fn cast_to_field_kind(&self, to: FieldKind) -> Result<Value, QueryError> {
        use Value::*;
        match to {
            FieldKind::Integer(_size, signed) => match (self, signed) {
                (&Unsigned(value), true )   => Ok(Signed(value as i128)),
                (&Unsigned(value), false)   => Ok(Unsigned(value)),
                (&Signed(value),   true )   => Ok(Signed(value)),
                (&Signed(value),   false)   => Ok(Unsigned(value as u128)),
                (&Real(value),     true )   => Ok(Signed(value as i128)),
                _ => Err(QueryError::IncompatibleTypes)
            },
            FieldKind::Real => match self {
                &Unsigned(value)    => Ok(Real(value as f64)),
                &Signed(value)      => Ok(Real(value as f64)),
                &Real(value)        => Ok(Real(value)),
                _ => Err(QueryError::IncompatibleTypes)
            },
            FieldKind::Text => match self {
                &Boolean(value)     => Ok(Text(value.to_string())),
                &Unsigned(value)    => Ok(Text(value.to_string())),
                &Signed(value)      => Ok(Text(value.to_string())),
                &Real(value)        => Ok(Text(value.to_string())),
                &Text(ref value)    => Ok(Text(value.clone())),
                _ => Err(QueryError::IncompatibleTypes)
            },
            _ => Err(QueryError::IncompatibleTypes),
        }
    }
}
