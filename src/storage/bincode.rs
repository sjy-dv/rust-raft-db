use crate::error::Result;

use bincode::Options;
use lazy_static::lazy_static;

lazy_static! {
    /// Create a static binding for the default Bincode options.
    static ref BINCODE: bincode::DefaultOptions = bincode::DefaultOptions::new();
}

/// Deserializes a value using Bincode.
pub fn deserialize<'de, T: serde::Deserialize<'de>>(bytes: &'de [u8]) -> Result<T> {
    Ok(BINCODE.deserialize(bytes)?)
}

/// Serializes a value using Bincode.
pub fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>> {
    Ok(BINCODE.serialize(value)?)
}