use FieldKind;
use CastError;

/// Types for data processing
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Unsigned(u128),
    Signed(i128),
    /// IEEE 754 double-precision binary floating-point format (binary64)
    Real(f64),
    /// UTF-8 Text
    Text(String),
    /// Arbitrary binary data
    Blob(Vec<u8>),
}
impl Value {
    pub fn simple_cast(&self, to: FieldKind) -> Result<Value, CastError> {
        use Value::*;
        match to {
            FieldKind::Integer(_size, signed) => match (self, signed) {
                (&Unsigned(value), true )   => Ok(Signed(value as i128)),
                (&Unsigned(value), false)   => Ok(Unsigned(value)),
                (&Signed(value),   true )   => Ok(Signed(value)),
                (&Signed(value),   false)   => Ok(Unsigned(value as u128)),
                (&Real(value),     true )   => Ok(Signed(value as i128)),
                _ => Err(CastError::InvalidTypes)
            },
            FieldKind::Real => match self {
                &Unsigned(value)    => Ok(Real(value as f64)),
                &Signed(value)      => Ok(Real(value as f64)),
                &Real(value)        => Ok(Real(value)),
                _ => Err(CastError::InvalidTypes)
            },
            FieldKind::Text => match self {
                &Unsigned(value)    => Ok(Text(value.to_string())),
                &Signed(value)      => Ok(Text(value.to_string())),
                &Real(value)        => Ok(Text(value.to_string())),
                &Text(ref value)    => Ok(Text(value.clone())),
                _ => Err(CastError::InvalidTypes)
            },
            _ => Err(CastError::InvalidTypes),
        }
    }
}
