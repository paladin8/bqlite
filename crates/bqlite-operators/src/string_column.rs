//! Layout-agnostic view over BqlType::String columns.
//!
//! Scan output may materialize a String column as either:
//!
//! - `StringViewArray` (Utf8View) — the default dense layout.
//! - `DictionaryArray<UInt8, Utf8View>` — emitted when the source
//!   row-group is dict-encoded with `code_bit_width <= 8`, so the dict
//!   codes can ride through the pipeline and let operators key on a
//!   `u8` instead of hashing the underlying string per row.
//!
//! Operators that want the string value but don't care about the
//! underlying layout resolve a `StringColumnView` once per batch and
//! call `value(row)` / `is_null(row)` in the hot loop. The dispatch
//! cost is one match per row — cheaper than re-downcasting per row.
//!
//! For operators that *do* want to exploit the dict layout (notably
//! `HashAccumulator` for low-cardinality `GROUP BY`), match against
//! the enum directly and read the keys array.

use arrow::array::{Array, DictionaryArray, StringViewArray, UInt8Array};
use arrow::datatypes::UInt8Type;

/// View over a BqlType::String column in either of the two supported
/// Arrow layouts. Resolved once per batch via [`StringColumnView::resolve`].
pub enum StringColumnView<'a> {
    /// Dense `StringViewArray` — the canonical Utf8View layout.
    View(&'a StringViewArray),
    /// Dict-coded `DictionaryArray<UInt8, Utf8View>` — emitted by the
    /// scan boundary for low-cardinality dict-encoded source columns.
    Dict {
        keys: &'a UInt8Array,
        values: &'a StringViewArray,
    },
}

impl<'a> StringColumnView<'a> {
    /// Resolve `arr` into a `StringColumnView`. Returns `None` if the
    /// Arrow type is neither `Utf8View` nor `Dictionary<UInt8, Utf8View>`.
    pub fn resolve(arr: &'a dyn Array) -> Option<Self> {
        if let Some(sv) = arr.as_any().downcast_ref::<StringViewArray>() {
            return Some(Self::View(sv));
        }
        if let Some(dict) = arr.as_any().downcast_ref::<DictionaryArray<UInt8Type>>() {
            if let Some(values) = dict.values().as_any().downcast_ref::<StringViewArray>() {
                return Some(Self::Dict {
                    keys: dict.keys(),
                    values,
                });
            }
        }
        None
    }

    /// True iff the row is null. For the dict layout a row is null if
    /// either its key is null or the referenced dict entry is null.
    #[inline]
    pub fn is_null(&self, row: usize) -> bool {
        match self {
            Self::View(sv) => sv.is_null(row),
            Self::Dict { keys, values } => {
                if keys.is_null(row) {
                    return true;
                }
                let code = keys.value(row) as usize;
                values.is_null(code)
            }
        }
    }

    /// Returns the string value at `row`. Callers must ensure the row
    /// is non-null (`is_null(row) == false`); passing a null row panics
    /// for the View layout and may return garbage / panic for Dict.
    #[inline]
    pub fn value(&self, row: usize) -> &str {
        match self {
            Self::View(sv) => sv.value(row),
            Self::Dict { keys, values } => {
                let code = keys.value(row) as usize;
                values.value(code)
            }
        }
    }

    /// Number of rows in the column.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::View(sv) => sv.len(),
            Self::Dict { keys, .. } => keys.len(),
        }
    }

    /// True iff the column has zero rows.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{DictionaryArray, StringViewArray, UInt8Array};
    use arrow::datatypes::UInt8Type;

    #[test]
    fn resolves_string_view_array() {
        let arr = StringViewArray::from(vec![Some("a"), None, Some("c")]);
        let view = StringColumnView::resolve(&arr).expect("resolves");
        assert!(matches!(view, StringColumnView::View(_)));
        assert_eq!(view.len(), 3);
        assert_eq!(view.value(0), "a");
        assert!(view.is_null(1));
        assert_eq!(view.value(2), "c");
    }

    #[test]
    fn resolves_dictionary_uint8() {
        // dict values: ["x", "y"], keys: [0, 1, 0]
        let values = Arc::new(StringViewArray::from(vec!["x", "y"])) as Arc<dyn Array>;
        let keys = UInt8Array::from(vec![0u8, 1, 0]);
        let dict =
            DictionaryArray::<UInt8Type>::try_new(keys, values).expect("dict construction works");
        let view = StringColumnView::resolve(&dict).expect("resolves");
        assert!(matches!(view, StringColumnView::Dict { .. }));
        assert_eq!(view.len(), 3);
        assert_eq!(view.value(0), "x");
        assert_eq!(view.value(1), "y");
        assert_eq!(view.value(2), "x");
        assert!(!view.is_null(0));
    }

    #[test]
    fn dict_null_via_key() {
        let values = Arc::new(StringViewArray::from(vec!["x", "y"])) as Arc<dyn Array>;
        let keys = UInt8Array::from(vec![Some(0u8), None, Some(1)]);
        let dict =
            DictionaryArray::<UInt8Type>::try_new(keys, values).expect("dict construction works");
        let view = StringColumnView::resolve(&dict).expect("resolves");
        assert!(!view.is_null(0));
        assert!(view.is_null(1));
        assert!(!view.is_null(2));
    }

    #[test]
    fn dict_null_via_value() {
        let values = Arc::new(StringViewArray::from(vec![Some("x"), None])) as Arc<dyn Array>;
        let keys = UInt8Array::from(vec![0u8, 1, 0]);
        let dict =
            DictionaryArray::<UInt8Type>::try_new(keys, values).expect("dict construction works");
        let view = StringColumnView::resolve(&dict).expect("resolves");
        assert!(!view.is_null(0));
        assert!(view.is_null(1));
        assert!(!view.is_null(2));
    }
}
