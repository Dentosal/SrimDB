use TableName;

#[derive(Debug, Clone)]
pub enum Field {
    /// Integer(size, signed)
    Integer(IntSize, bool),
    /// IEEE 754 double-precision binary floating-point format (binary64)
    Real,
    /// UTF-8 Text
    Text,
    /// Arbitrary binary data
    Blob,
    /// Foreign Key
    ForeignKey(Vec<Field>),
}

/// Types for data storage / annotation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldKind {
    /// Integer(size, signed)
    Integer(IntSize, bool),
    /// IEEE 754 double-precision binary floating-point format (binary64)
    Real,
    /// UTF-8 Text
    Text,
    /// Arbitrary binary data
    Blob,
    /// Arbitrary binary data
    ForeignKey(TableName),
}
impl FieldKind {
    pub fn constant_size_bytes(self) -> Option<u8> {
        if let FieldKind::Integer(size, _) = self {
            Some(size.size_bytes())
        }
        else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntSize {
    N8,   // [iu]8
    N16,  // [iu]16
    N32,  // [iu]32
    N64,  // [iu]64
    N128, // [iu]128
}
impl IntSize {
    pub fn size_bytes(self) -> u8 {
        use self::IntSize::*;
        match self {
            N8   => 1,
            N16  => 2,
            N32  => 4,
            N64  => 8,
            N128 => 16,
        }
    }
}
