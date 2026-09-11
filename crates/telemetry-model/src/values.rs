//! The value union and attribute maps.
//!
//! A value is exactly one of: string, boolean, 64-bit integer, 64-bit float,
//! bytes, array, or key-value list (`docs/architecture/telemetry-model.md`,
//! "Values and attributes"). Integers and floats are never interconverted;
//! NaN and the infinities are preserved values; empty is a value and is
//! distinct from the key being absent.

use std::collections::BTreeMap;
use std::fmt;
use std::hash::{Hash, Hasher};

/// A 64-bit float that compares by bit pattern.
///
/// The model's comparison is *total* over doubles: NaN equals itself, the
/// two infinities are distinct values, and `-0.0` differs from `0.0`. This
/// is what makes record identity (duplicate delivery, conflict detection)
/// well-defined for payloads that carry floats — a re-delivered NaN is the
/// same value, not an incomparable one. The bits are the preserved value;
/// numeric comparison is a view concern and is not modelled here.
#[derive(Clone, Copy, Debug)]
pub struct Float(f64);

impl Float {
    /// Wraps a raw `f64` verbatim — no normalisation, no canonicalisation.
    #[must_use]
    pub const fn new(value: f64) -> Self {
        Self(value)
    }

    /// The wrapped `f64`, exactly as wrapped.
    #[must_use]
    pub const fn get(self) -> f64 {
        self.0
    }

    /// The IEEE-754 bit pattern of the wrapped value.
    #[must_use]
    pub const fn bits(self) -> u64 {
        self.0.to_bits()
    }
}

impl PartialEq for Float {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for Float {}

impl Hash for Float {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

/// One primitive value — the element kind arrays are made of.
///
/// Array elements are of one primitive kind
/// (`docs/architecture/telemetry-model.md`, "Values and attributes"); a
/// key-value list is not a primitive, so an array of key-value lists is not
/// a representable value and is rejected where the wire carries one.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum PrimitiveValue {
    String(String),
    Bool(bool),
    Int(i64),
    Double(Float),
    Bytes(Vec<u8>),
}

impl PrimitiveValue {
    /// The kind of this value; the kind every element of a homogeneous
    /// array shares.
    #[must_use]
    pub const fn kind(&self) -> PrimitiveKind {
        match self {
            Self::String(_) => PrimitiveKind::String,
            Self::Bool(_) => PrimitiveKind::Bool,
            Self::Int(_) => PrimitiveKind::Int,
            Self::Double(_) => PrimitiveKind::Double,
            Self::Bytes(_) => PrimitiveKind::Bytes,
        }
    }
}

/// The primitive kinds an array's elements may share.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PrimitiveKind {
    String,
    Bool,
    Int,
    Double,
    Bytes,
}

/// A mixed-kind array was offered where one primitive kind was required.
///
/// A mixed-kind array is invalid input: it is rejected, not coerced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MixedKindArray {
    /// The kind the array's first element established.
    pub first_kind: PrimitiveKind,
    /// Position of the element that broke homogeneity.
    pub offending_index: usize,
    /// The offending element's kind.
    pub offending_kind: PrimitiveKind,
}

impl fmt::Display for MixedKindArray {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "mixed-kind array: element {} is {:?} in an array of {:?}",
            self.offending_index, self.offending_kind, self.first_kind
        )
    }
}

impl std::error::Error for MixedKindArray {}

/// An array whose elements all carry one primitive kind.
///
/// Constructed only through [`HomogeneousArray::try_from_items`], which
/// rejects a mixed-kind array instead of coercing it. An empty array is a
/// value: it is distinct from the key being absent, and it carries no kind.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HomogeneousArray {
    items: Vec<PrimitiveValue>,
}

impl HomogeneousArray {
    /// Builds a homogeneous array, or rejects a mixed-kind one.
    ///
    /// # Errors
    ///
    /// Returns [`MixedKindArray`] when the items do not all carry the same
    /// primitive kind. The input is never coerced or shrunk.
    pub fn try_from_items(items: Vec<PrimitiveValue>) -> Result<Self, MixedKindArray> {
        let Some(first) = items.first() else {
            return Ok(Self { items });
        };
        let first_kind = first.kind();
        for (index, item) in items.iter().enumerate().skip(1) {
            if item.kind() != first_kind {
                return Err(MixedKindArray {
                    first_kind,
                    offending_index: index,
                    offending_kind: item.kind(),
                });
            }
        }
        Ok(Self { items })
    }

    /// The shared element kind; `None` for an empty array.
    #[must_use]
    pub fn kind(&self) -> Option<PrimitiveKind> {
        self.items.first().map(PrimitiveValue::kind)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, PrimitiveValue> {
        self.items.iter()
    }
}

/// A key-value list — an *ordered* map, as the model contract states it.
///
/// The entry order the emitter sent is preserved verbatim and is part of the
/// value's identity. Map semantics apply within the list: a duplicated key
/// keeps its first occurrence (the first thing admitted stands, mirroring
/// record identity), and every occurrence is preserved as sent.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct KeyValueList {
    entries: Vec<(String, Value)>,
}

impl KeyValueList {
    /// Builds a key-value list, preserving emitter order. A duplicated key
    /// keeps its first occurrence; nothing is reordered or dropped.
    #[must_use]
    pub fn new(entries: Vec<(String, Value)>) -> Self {
        let mut kept: Vec<(String, Value)> = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            if kept.iter().any(|(existing, _)| existing == &key) {
                continue; // keep-first: a duplicated key adds no entry
            }
            kept.push((key, value));
        }
        Self { entries: kept }
    }

    /// The first value carried under `key`, if any.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.entries
            .iter()
            .find(|(existing, _)| existing == key)
            .map(|(_, value)| value)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, (String, Value)> {
        self.entries.iter()
    }
}

/// An attribute map: keys to values, compared as a map.
///
/// Attribute order is not semantic (unlike a [`KeyValueList`]'s entry
/// order): two maps are equal exactly when they carry the same keys with
/// equal values, however the emitter batched its exports. A duplicated key
/// keeps its first occurrence.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Attributes {
    entries: BTreeMap<String, Value>,
}

impl Attributes {
    /// Builds a map from emitter-ordered pairs. A duplicated key keeps its
    /// first occurrence; nothing else is rewritten.
    #[must_use]
    pub fn from_pairs(pairs: Vec<(String, Value)>) -> Self {
        let mut entries = BTreeMap::new();
        for (key, value) in pairs {
            // keep-first: the first occurrence under a key stands, the
            // same first-admitted-wins rule record identity follows.
            entries.entry(key).or_insert(value);
        }
        Self { entries }
    }

    /// The value carried under `key`, if the key is present. An empty value
    /// under a present key is a value — only a missing key is absent.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.entries.get(key)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, String, Value> {
        self.entries.iter()
    }
}

impl<'a> IntoIterator for &'a HomogeneousArray {
    type Item = &'a PrimitiveValue;
    type IntoIter = std::slice::Iter<'a, PrimitiveValue>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

impl<'a> IntoIterator for &'a KeyValueList {
    type Item = &'a (String, Value);
    type IntoIter = std::slice::Iter<'a, (String, Value)>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

impl<'a> IntoIterator for &'a Attributes {
    type Item = (&'a String, &'a Value);
    type IntoIter = std::collections::btree_map::Iter<'a, String, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

/// The model's value union: exactly one of string, boolean, 64-bit integer,
/// 64-bit float, bytes, array, or key-value list.
///
/// Integers and floats are distinct variants and never interconverted;
/// doubles compare by bit pattern (see [`Float`]); an empty string, an
/// empty array, an empty key-value list, an empty byte string, zero and
/// false are each values, distinct from the key being absent.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Value {
    String(String),
    Bool(bool),
    Int(i64),
    Double(Float),
    Bytes(Vec<u8>),
    Array(HomogeneousArray),
    KvList(KeyValueList),
}

impl Value {
    /// Builds an array value, rejecting a mixed-kind array instead of
    /// coercing it.
    ///
    /// # Errors
    ///
    /// Returns [`MixedKindArray`] when the items do not all carry one
    /// primitive kind.
    pub fn array(items: Vec<PrimitiveValue>) -> Result<Self, MixedKindArray> {
        Ok(Self::Array(HomogeneousArray::try_from_items(items)?))
    }

    /// Builds a key-value list value, preserving emitter entry order.
    #[must_use]
    pub fn kv_list(entries: Vec<(String, Value)>) -> Self {
        Self::KvList(KeyValueList::new(entries))
    }

    /// The deepest chain of nested key-value lists reachable from this
    /// value. A key-value list at the top counts as depth 1; a key-value
    /// list inside it counts as 2. This is the count the
    /// key-value-list-depth budget gates on.
    #[must_use]
    pub fn kv_depth(&self) -> usize {
        match self {
            // Scalars never deepen a chain, and array elements are
            // primitives (the constructor rejects anything else), so an
            // array never carries a key-value list to deepen one either.
            Self::String(_)
            | Self::Bool(_)
            | Self::Int(_)
            | Self::Double(_)
            | Self::Bytes(_)
            | Self::Array(_) => 0,
            Self::KvList(list) => {
                1 + list
                    .iter()
                    .map(|(_, value)| value.kv_depth())
                    .max()
                    .unwrap_or(0)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string_value(text: &str) -> Value {
        Value::String(text.to_owned())
    }

    #[test]
    fn integers_and_floats_are_never_interconverted() {
        assert_ne!(Value::Int(1), Value::Double(Float::new(1.0)));
        assert_ne!(Value::Int(0), Value::Double(Float::new(0.0)));
    }

    #[test]
    fn nan_compares_equal_to_itself_by_bits() {
        let nan = Value::Double(Float::new(f64::NAN));
        assert_eq!(nan, nan.clone());
        assert_ne!(nan, Value::Double(Float::new(f64::INFINITY)));
    }

    #[test]
    fn the_two_infinities_are_distinct_preserved_values() {
        let positive = Value::Double(Float::new(f64::INFINITY));
        let negative = Value::Double(Float::new(f64::NEG_INFINITY));
        assert_ne!(positive, negative);
        assert_eq!(positive, positive.clone());
    }

    #[test]
    fn the_sign_of_zero_is_a_preserved_distinction() {
        assert_ne!(
            Value::Double(Float::new(0.0)),
            Value::Double(Float::new(-0.0))
        );
    }

    #[test]
    fn a_mixed_kind_array_is_rejected_not_coerced() {
        let mixed = vec![
            PrimitiveValue::Int(1),
            PrimitiveValue::String("two".to_owned()),
        ];
        let error = Value::array(mixed).expect_err("a mixed-kind array is invalid input");
        assert_eq!(error.offending_index, 1);
        assert_eq!(error.first_kind, PrimitiveKind::Int);
        assert_eq!(error.offending_kind, PrimitiveKind::String);
    }

    #[test]
    fn a_homogeneous_array_is_admitted_and_keeps_its_kind() {
        let items = vec![PrimitiveValue::Int(7), PrimitiveValue::Int(8)];
        let array = Value::array(items).expect("one kind");
        assert_eq!(
            array,
            Value::Array(
                HomogeneousArray::try_from_items(vec![
                    PrimitiveValue::Int(7),
                    PrimitiveValue::Int(8),
                ])
                .expect("one kind")
            )
        );
        let Value::Array(ref inner) = array else {
            panic!("array variant expected");
        };
        assert_eq!(inner.kind(), Some(PrimitiveKind::Int));
    }

    #[test]
    fn an_empty_array_is_a_value_distinct_from_absence() {
        let empty = Value::array(Vec::new()).expect("empty is homogeneous");
        let Value::Array(ref inner) = empty else {
            panic!("array variant expected");
        };
        assert!(inner.is_empty());
        assert_eq!(inner.kind(), None);
        let map = Attributes::from_pairs(vec![("a".to_owned(), empty.clone())]);
        assert!(map.get("a").is_some(), "present with an empty value");
        assert!(map.get("b").is_none(), "absent key");
    }

    #[test]
    fn empty_string_zero_and_false_are_values_under_present_keys() {
        let map = Attributes::from_pairs(vec![
            ("empty".to_owned(), string_value("")),
            ("zero".to_owned(), Value::Int(0)),
            ("false".to_owned(), Value::Bool(false)),
            ("empty_bytes".to_owned(), Value::Bytes(Vec::new())),
            ("empty_list".to_owned(), Value::kv_list(Vec::new())),
        ]);
        assert_eq!(map.get("empty"), Some(&string_value("")));
        assert_eq!(map.get("zero"), Some(&Value::Int(0)));
        assert_eq!(map.get("false"), Some(&Value::Bool(false)));
        assert_eq!(map.get("empty_bytes"), Some(&Value::Bytes(Vec::new())));
        let empty_list = Value::kv_list(Vec::new());
        assert_eq!(map.get("empty_list"), Some(&empty_list));
        assert_eq!(map.len(), 5);
    }

    #[test]
    fn attribute_maps_compare_as_maps_regardless_of_pair_order() {
        let first = Attributes::from_pairs(vec![
            ("a".to_owned(), Value::Int(1)),
            ("b".to_owned(), Value::Int(2)),
        ]);
        let second = Attributes::from_pairs(vec![
            ("b".to_owned(), Value::Int(2)),
            ("a".to_owned(), Value::Int(1)),
        ]);
        assert_eq!(first, second);
    }

    #[test]
    fn a_duplicated_attribute_key_keeps_its_first_value() {
        let map = Attributes::from_pairs(vec![
            ("k".to_owned(), Value::Int(1)),
            ("k".to_owned(), Value::Int(2)),
        ]);
        assert_eq!(map.get("k"), Some(&Value::Int(1)));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn key_value_lists_preserve_emitter_order_and_it_is_identity() {
        let first = Value::kv_list(vec![
            ("a".to_owned(), Value::Int(1)),
            ("b".to_owned(), Value::Int(2)),
        ]);
        let reordered = Value::kv_list(vec![
            ("b".to_owned(), Value::Int(2)),
            ("a".to_owned(), Value::Int(1)),
        ]);
        assert_ne!(first, reordered, "entry order is semantic for a kvlist");
        let Value::KvList(ref list) = first else {
            panic!("kvlist variant expected");
        };
        let keys: Vec<&str> = list.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn a_duplicated_kvlist_key_keeps_its_first_occurrence_in_order() {
        let list = Value::kv_list(vec![
            ("k".to_owned(), Value::Int(1)),
            ("other".to_owned(), Value::Bool(true)),
            ("k".to_owned(), Value::Int(2)),
        ]);
        let Value::KvList(ref inner) = list else {
            panic!("kvlist variant expected");
        };
        assert_eq!(inner.len(), 2);
        assert_eq!(inner.get("k"), Some(&Value::Int(1)));
        assert_eq!(inner.get("other"), Some(&Value::Bool(true)));
    }

    #[test]
    fn kvlist_depth_counts_nested_lists() {
        let flat = Value::kv_list(vec![("a".to_owned(), Value::Int(1))]);
        assert_eq!(flat.kv_depth(), 1);
        let nested = Value::kv_list(vec![(
            "outer".to_owned(),
            Value::kv_list(vec![("inner".to_owned(), Value::kv_list(Vec::new()))]),
        )]);
        assert_eq!(nested.kv_depth(), 3);
        let scalar = Value::Int(5);
        assert_eq!(scalar.kv_depth(), 0);
    }
}
