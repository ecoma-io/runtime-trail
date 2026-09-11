//! The value union and attribute maps.
//!
//! A value is exactly one of: string, boolean, 64-bit integer, 64-bit float,
//! bytes, array, or key-value list (`docs/architecture/telemetry-model.md`,
//! "Values and attributes"). Integers and floats are never interconverted;
//! NaN and the infinities are preserved values; empty is a value and is
//! distinct from the key being absent.
//!
//! Arrays are homogeneous in *value kind*: every element carries the same
//! one of the seven kinds — including arrays of arrays and arrays of
//! key-value lists, which OTLP carries and structured log bodies use. A
//! mixed-kind array is still invalid input, refused, not coerced.
//!
//! Duplicated keys are invalid input everywhere a keyed container is built:
//! the previous keep-first behaviour silently dropped a value the emitter
//! sent, and the contract forbids silent loss (`docs/architecture/
//! telemetry-model.md`, "Conformance language"). Both [`Attributes`] and
//! [`KeyValueList`] refuse a duplicate key with [`DuplicateKey`].
//!
//! # Recursion safety
//!
//! Every recursive walk over a [`Value`] in this crate — nesting checks,
//! accounted size, and drop — is an iterative traversal over an explicit
//! stack, so a deeply nested value (legal on the wire until the depth gate
//! refuses it) cannot overflow the stack. See [`Value::exceeds_nesting`]
//! and `crate::size`.

use std::collections::{BTreeMap, btree_map};
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

/// The kind of a value: exactly one of the seven kinds in the union.
///
/// Array homogeneity is defined over this enum, so an array of key-value
/// lists or an array of arrays is legal (its element kind is
/// [`ValueKind::KvList`] or [`ValueKind::Array`]); a mixed-kind array is
/// refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ValueKind {
    String,
    Bool,
    Int,
    Double,
    Bytes,
    Array,
    KvList,
}

/// A duplicated key was offered where keys must be unique.
///
/// Any loss must be refused or recorded — never silent
/// (`docs/architecture/telemetry-model.md`, "Conformance language"). The
/// previous keep-first behaviour silently dropped every occurrence of a
/// duplicated key after the first; now the whole container construction is
/// refused as invalid input, the same policy mixed-kind arrays follow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuplicateKey {
    /// The key that appeared more than once.
    pub key: String,
}

impl fmt::Display for DuplicateKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "duplicate key {:?}: a duplicated key would silently drop a sent value",
            self.key
        )
    }
}

impl std::error::Error for DuplicateKey {}

/// A mixed-kind array was offered where one value kind was required.
///
/// A mixed-kind array is invalid input: it is rejected, not coerced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MixedKindArray {
    /// The kind the array's first element established.
    pub first_kind: ValueKind,
    /// Position of the element that broke homogeneity.
    pub offending_index: usize,
    /// The offending element's kind.
    pub offending_kind: ValueKind,
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

/// An array whose elements all carry one value kind.
///
/// Homogeneity is over the full value union: the element kind may be any
/// one of the seven kinds, including [`ValueKind::KvList`] and
/// [`ValueKind::Array`] (OTLP `ArrayValue` is a repeated `AnyValue`, and
/// arrays of key-value lists are common structured-log bodies). An empty
/// array is a value: it is distinct from the key being absent, and it
/// carries no kind.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HomogeneousArray {
    items: Vec<Value>,
}

impl HomogeneousArray {
    /// Builds a homogeneous array, or rejects a mixed-kind one.
    ///
    /// # Errors
    ///
    /// Returns [`MixedKindArray`] when the items do not all carry the same
    /// value kind. The input is never coerced or shrunk.
    pub fn try_from_items(items: Vec<Value>) -> Result<Self, MixedKindArray> {
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
    pub fn kind(&self) -> Option<ValueKind> {
        self.items.first().map(Value::kind)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Value> {
        self.items.iter()
    }
}

impl<'a> IntoIterator for &'a HomogeneousArray {
    type Item = &'a Value;
    type IntoIter = std::slice::Iter<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.items.iter()
    }
}

impl Drop for HomogeneousArray {
    /// Iterative, for the same reason as [`KeyValueList::drop`]: an array
    /// chain the depth gate refused must drop in a loop, not recurse
    /// through one `Vec` frame per nesting level.
    fn drop(&mut self) {
        drop_values(std::mem::take(&mut self.items));
    }
}

/// Drops a batch of values — and everything nested inside them —
/// iteratively.
///
/// Rust's derived drop glue recurses once per nesting level, so a value
/// tens of thousands of levels deep (legal on the wire until the depth
/// gate refuses it) would overflow the stack while being dropped. This
/// flattens the tree with an explicit stack instead: dropping a refused
/// deep value is a loop, never a recursion.
fn drop_values(values: Vec<Value>) {
    let mut stack: Vec<Value> = values;
    let mut lists: Vec<Vec<(String, Value)>> = Vec::new();
    while let Some(value) = stack.pop() {
        match value {
            Value::KvList(mut list) => lists.push(std::mem::take(&mut list.entries)),
            Value::Array(mut array) => stack.extend(std::mem::take(&mut array.items)),
            other => drop(other),
        }
        // Drain one pending key-value list per outer step so neither stack
        // grows without bound on wide-and-deep shapes.
        while let Some(entries) = lists.pop() {
            for (key, value) in entries {
                drop(key);
                stack.push(value);
            }
        }
    }
}

/// A key-value list — an *ordered* map, as the model contract states it.
///
/// The entry order the emitter sent is preserved verbatim and is part of the
/// value's identity. Map semantics apply within the list — but a duplicated
/// key is invalid input, refused with [`DuplicateKey`]: keeping the first
/// occurrence would silently drop a value the emitter sent.
///
/// Dropping a deeply nested list is iterative (see [`Self::drop`]), so a
/// value the depth gate refused cannot overflow the stack on its way out.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct KeyValueList {
    entries: Vec<(String, Value)>,
}

impl Drop for KeyValueList {
    /// Iterative, by contract: the derived drop glue recurses once per
    /// nesting level, which is exactly the stack overflow the depth gate
    /// exists to prevent. Refused deep values are dropped by the caller
    /// after the gate refuses them, so this must never recurse.
    fn drop(&mut self) {
        let mut stack: Vec<Value> = Vec::new();
        for (key, value) in std::mem::take(&mut self.entries) {
            drop(key);
            stack.push(value);
        }
        drop_values(stack);
    }
}

impl KeyValueList {
    /// Builds a key-value list, preserving emitter order.
    ///
    /// # Errors
    ///
    /// Returns [`DuplicateKey`] when a key appears more than once. The
    /// input is never coerced, reordered, or partially kept. Insertion is
    /// a hashed set membership test per entry — linear in the entry count,
    /// never the quadratic scan the keep-first behaviour used.
    pub fn new(entries: Vec<(String, Value)>) -> Result<Self, DuplicateKey> {
        let kept: Vec<(String, Value)> = entries;
        let mut seen: std::collections::HashSet<&str> =
            std::collections::HashSet::with_capacity(kept.len());
        for (key, _) in &kept {
            if !seen.insert(key.as_str()) {
                return Err(DuplicateKey { key: key.clone() });
            }
        }
        Ok(Self { entries: kept })
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

impl<'a> IntoIterator for &'a KeyValueList {
    type Item = &'a (String, Value);
    type IntoIter = std::slice::Iter<'a, (String, Value)>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

/// An attribute map: keys to values, compared as a map.
///
/// Attribute order is not semantic (unlike a [`KeyValueList`]'s entry
/// order): two maps are equal exactly when they carry the same keys with
/// equal values, however the emitter batched its exports. A duplicated key
/// is invalid input, refused with [`DuplicateKey`] — never silently
/// kept-first.
///
/// The map is a [`BTreeMap`]; its node cost is what [`crate::size`]'s
/// per-map base and per-entry charges cover.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct Attributes {
    entries: BTreeMap<String, Value>,
}

impl Attributes {
    /// Builds a map from emitter-ordered pairs.
    ///
    /// # Errors
    ///
    /// Returns [`DuplicateKey`] when a key appears more than once. The
    /// check is a B-tree insert-and-test — `O(log n)` per entry, which
    /// replaces the quadratic scan the keep-first behaviour used.
    pub fn from_pairs(pairs: Vec<(String, Value)>) -> Result<Self, DuplicateKey> {
        let mut entries = BTreeMap::new();
        for (key, value) in pairs {
            match entries.entry(key) {
                btree_map::Entry::Occupied(existing) => {
                    return Err(DuplicateKey {
                        key: existing.key().clone(),
                    });
                }
                btree_map::Entry::Vacant(slot) => {
                    slot.insert(value);
                }
            }
        }
        Ok(Self { entries })
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
///
/// Arrays are homogeneous in *value kind* — the element kind may be any of
/// the seven kinds, arrays and key-value lists included; a mixed-kind array
/// is refused at construction. Key-value lists refuse duplicated keys at
/// construction.
///
/// Every value carries at most seven kinds of recursion through its
/// nesting; the walks this crate runs over values (nesting depth, accounted
/// size, drop) are all iterative, so a deeply nested value cannot overflow
/// the stack before the depth gate refuses it.
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
    /// value kind.
    pub fn array(items: Vec<Value>) -> Result<Self, MixedKindArray> {
        Ok(Self::Array(HomogeneousArray::try_from_items(items)?))
    }

    /// Builds a key-value list value, preserving emitter entry order.
    ///
    /// # Errors
    ///
    /// Returns [`DuplicateKey`] when a key appears more than once.
    pub fn kv_list(entries: Vec<(String, Value)>) -> Result<Self, DuplicateKey> {
        Ok(Self::KvList(KeyValueList::new(entries)?))
    }

    /// The kind of this value — the discriminator array homogeneity and
    /// the accounted-size walk both read.
    #[must_use]
    pub const fn kind(&self) -> ValueKind {
        match self {
            Self::String(_) => ValueKind::String,
            Self::Bool(_) => ValueKind::Bool,
            Self::Int(_) => ValueKind::Int,
            Self::Double(_) => ValueKind::Double,
            Self::Bytes(_) => ValueKind::Bytes,
            Self::Array(_) => ValueKind::Array,
            Self::KvList(_) => ValueKind::KvList,
        }
    }

    /// Whether the nesting depth of this value exceeds `limit`.
    ///
    /// Every container level counts — a key-value list adds one, and so
    /// does an array, because the depth budget exists to bound every
    /// recursive structure a record may carry: an admitted value is at
    /// most `key_value_list_depth` levels deep *however* it nests, which
    /// is the guarantee that the derived `PartialEq`/`Debug`/`Clone` glue
    /// the admission ledger and storage run over *admitted* records is
    /// bounded. (`Drop` is iterative regardless — see
    /// [`KeyValueList::drop`] — because the walk that refuses an over-deep
    /// value must be able to drop it afterwards.)
    ///
    /// The traversal is iterative over an explicit stack and stops at the
    /// first level past `limit`: a value deeper than the limit is refused
    /// without walking the rest of it.
    #[must_use]
    pub fn exceeds_nesting(&self, limit: usize) -> bool {
        let mut stack: Vec<(&Value, usize)> = vec![(self, 0)];
        while let Some((value, below)) = stack.pop() {
            let depth = match value {
                Value::KvList(_) | Value::Array(_) => below + 1,
                _ => continue,
            };
            if depth > limit {
                return true;
            }
            match value {
                Value::KvList(list) => {
                    for (_, child) in list {
                        stack.push((child, depth));
                    }
                }
                Value::Array(array) => {
                    for child in array {
                        stack.push((child, depth));
                    }
                }
                _ => unreachable!("only containers carry a depth"),
            }
        }
        false
    }

    /// The full nesting depth of this value: the deepest chain of nested
    /// containers (key-value lists and arrays alike), where a top-level
    /// container counts as depth 1. Iterative over an explicit stack.
    #[must_use]
    pub fn nesting_depth(&self) -> usize {
        let mut deepest = 0;
        let mut stack: Vec<(&Value, usize)> = vec![(self, 0)];
        while let Some((value, below)) = stack.pop() {
            let depth = match value {
                Value::KvList(_) | Value::Array(_) => below + 1,
                _ => continue,
            };
            deepest = deepest.max(depth);
            match value {
                Value::KvList(list) => {
                    for (_, child) in list {
                        stack.push((child, depth));
                    }
                }
                Value::Array(array) => {
                    for child in array {
                        stack.push((child, depth));
                    }
                }
                _ => unreachable!("only containers carry a depth"),
            }
        }
        deepest
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn string_value(text: &str) -> Value {
        Value::String(text.to_owned())
    }

    fn int(i: i64) -> Value {
        Value::Int(i)
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
        let mixed = vec![int(1), string_value("two")];
        let error = Value::array(mixed).expect_err("a mixed-kind array is invalid input");
        assert_eq!(error.offending_index, 1);
        assert_eq!(error.first_kind, ValueKind::Int);
        assert_eq!(error.offending_kind, ValueKind::String);
    }

    #[test]
    fn a_homogeneous_array_is_admitted_and_keeps_its_kind() {
        let array = Value::array(vec![int(7), int(8)]).expect("one kind");
        let Value::Array(ref inner) = array else {
            panic!("array variant expected");
        };
        assert_eq!(inner.kind(), Some(ValueKind::Int));
    }

    #[test]
    fn an_array_of_key_value_lists_is_a_legal_value() {
        let entries = |k: &str, i: i64| Value::kv_list(vec![(k.to_owned(), int(i))]).expect("ok");
        let array = Value::array(vec![entries("a", 1), entries("b", 2)]).expect("one kind: kvlist");
        let Value::Array(ref inner) = array else {
            panic!("array variant expected");
        };
        assert_eq!(inner.kind(), Some(ValueKind::KvList));
        assert_eq!(inner.len(), 2);
        assert_eq!(array.nesting_depth(), 2, "the array plus its kvlist items");
    }

    #[test]
    fn an_array_of_arrays_is_a_legal_value() {
        let inner = Value::array(vec![int(1), int(2)]).expect("one kind");
        let array = Value::array(vec![inner.clone(), inner]).expect("one kind: array");
        let Value::Array(ref outer) = array else {
            panic!("array variant expected");
        };
        assert_eq!(outer.kind(), Some(ValueKind::Array));
        assert_eq!(array.nesting_depth(), 2);
    }

    #[test]
    fn a_string_and_a_kvlist_cannot_share_an_array() {
        let kvlist = Value::kv_list(vec![("a".to_owned(), int(1))]).expect("ok");
        let mixed = vec![string_value("s"), kvlist];
        let error = Value::array(mixed).expect_err("mixed kinds are refused");
        assert_eq!(error.first_kind, ValueKind::String);
        assert_eq!(error.offending_kind, ValueKind::KvList);
        assert_eq!(error.offending_index, 1);
    }

    #[test]
    fn an_empty_array_is_a_value_distinct_from_absence() {
        let empty = Value::array(Vec::new()).expect("empty is homogeneous");
        let Value::Array(ref inner) = empty else {
            panic!("array variant expected");
        };
        assert!(inner.is_empty());
        assert_eq!(inner.kind(), None);
        let map = Attributes::from_pairs(vec![("a".to_owned(), empty.clone())]).expect("ok");
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
            (
                "empty_list".to_owned(),
                Value::kv_list(Vec::new()).expect("ok"),
            ),
        ])
        .expect("ok");
        assert_eq!(map.get("empty"), Some(&string_value("")));
        assert_eq!(map.get("zero"), Some(&Value::Int(0)));
        assert_eq!(map.get("false"), Some(&Value::Bool(false)));
        assert_eq!(map.get("empty_bytes"), Some(&Value::Bytes(Vec::new())));
        let empty_list = Value::kv_list(Vec::new()).expect("ok");
        assert_eq!(map.get("empty_list"), Some(&empty_list));
        assert_eq!(map.len(), 5);
    }

    #[test]
    fn attribute_maps_compare_as_maps_regardless_of_pair_order() {
        let first = Attributes::from_pairs(vec![
            ("a".to_owned(), Value::Int(1)),
            ("b".to_owned(), Value::Int(2)),
        ])
        .expect("ok");
        let second = Attributes::from_pairs(vec![
            ("b".to_owned(), Value::Int(2)),
            ("a".to_owned(), Value::Int(1)),
        ])
        .expect("ok");
        assert_eq!(first, second);
    }

    #[test]
    fn a_duplicated_attribute_key_is_refused_not_kept_first() {
        let error = Attributes::from_pairs(vec![
            ("k".to_owned(), Value::Int(1)),
            ("other".to_owned(), Value::Bool(true)),
            ("k".to_owned(), Value::Int(2)),
        ])
        .expect_err("a duplicated key is invalid input");
        assert_eq!(error.key, "k");
        assert_eq!(
            error.to_string(),
            "duplicate key \"k\": a duplicated key would silently drop a sent value"
        );
    }

    #[test]
    fn a_duplicated_kvlist_key_is_refused_not_kept_first() {
        let error = Value::kv_list(vec![
            ("k".to_owned(), Value::Int(1)),
            ("k".to_owned(), Value::Int(2)),
        ])
        .expect_err("a duplicated key is invalid input");
        assert_eq!(error.key, "k");
    }

    #[test]
    fn duplicate_key_detection_is_linear_not_quadratic() {
        // 40k unique keys must build in well under the 9.5 s the old
        // keep-first scan needed for this shape; this asserts the O(n log n)
        // construction by timing out (test failure) if it ever regresses.
        let start = std::time::Instant::now();
        let pairs: Vec<(String, Value)> = (0..40_000)
            .map(|i| (format!("k{i}"), Value::Int(i)))
            .collect();
        let map = Attributes::from_pairs(pairs).expect("all keys unique");
        let elapsed = start.elapsed();
        assert_eq!(map.len(), 40_000);
        assert!(
            elapsed < std::time::Duration::from_secs(2),
            "building 40k attributes took {elapsed:?}; the duplicate check went quadratic"
        );
    }

    #[test]
    fn key_value_lists_preserve_emitter_order_and_it_is_identity() {
        let first = Value::kv_list(vec![
            ("a".to_owned(), Value::Int(1)),
            ("b".to_owned(), Value::Int(2)),
        ])
        .expect("ok");
        let reordered = Value::kv_list(vec![
            ("b".to_owned(), Value::Int(2)),
            ("a".to_owned(), Value::Int(1)),
        ])
        .expect("ok");
        assert_ne!(first, reordered, "entry order is semantic for a kvlist");
        let Value::KvList(ref list) = first else {
            panic!("kvlist variant expected");
        };
        let keys: Vec<&str> = list.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(keys, vec!["a", "b"]);
    }

    #[test]
    fn nesting_depth_counts_every_container_level() {
        let flat = Value::kv_list(vec![("a".to_owned(), Value::Int(1))]).expect("ok");
        assert_eq!(flat.nesting_depth(), 1);
        let nested = Value::kv_list(vec![(
            "outer".to_owned(),
            Value::kv_list(vec![(
                "inner".to_owned(),
                Value::kv_list(Vec::new()).expect("ok"),
            )])
            .expect("ok"),
        )])
        .expect("ok");
        assert_eq!(nested.nesting_depth(), 3);
        let scalar = Value::Int(5);
        assert_eq!(scalar.nesting_depth(), 0);
        // Arrays count too: a depth-8 array-of-kvlist chain is depth 8.
        let mut chain = Value::kv_list(vec![("leaf".to_owned(), int(0))]).expect("ok");
        for _ in 1..8 {
            chain = Value::array(vec![chain]).expect("one kind: array");
        }
        assert_eq!(chain.nesting_depth(), 8, "alternating array/kvlist chain");
        assert!(!chain.exceeds_nesting(8));
        assert!(chain.exceeds_nesting(7));
    }

    #[test]
    fn exceeds_nesting_stops_early_and_reports_deep_values() {
        // Built iteratively on purpose: a 60k-deep chain would abort the
        // test before the gate if construction or the check recursed.
        let mut deep = int(0);
        for _ in 0..60_000 {
            deep = Value::kv_list(vec![("n".to_owned(), deep)]).expect("ok");
        }
        assert!(deep.exceeds_nesting(8));
        assert_eq!(deep.nesting_depth(), 60_000);
        // The refused value drops iteratively: if this drop recursed, the
        // test would abort with a stack overflow instead of failing.
        drop(deep);
    }
}
