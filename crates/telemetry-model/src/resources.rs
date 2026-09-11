//! Resource and scope identity.
//!
//! `docs/architecture/telemetry-model.md`, "Resource and scope identity": a
//! resource is an attribute map and two records share a resource exactly
//! when their attribute maps are equal; a scope is identified by
//! (name, version, attributes) and its schema URL participates in that
//! identity; `schema_url` exists at two levels and both are preserved;
//! "service" is not a typed model field — it is the resource attribute
//! `service.name`.
//!
//! The resource's `schema_url` is preserved *metadata*: [`Resource`]'s
//! `PartialEq`/`Hash` compare the attribute map only, so every identity
//! built on the type (stream identity, duplicate detection, ledger keys)
//! inherits attribute-map equality and never a `schema_url` coincidence.
//! The full-field comparison lives in [`Resource::identical`], clearly
//! named for the rare caller that wants byte equality of the whole struct.

use crate::values::Attributes;
use std::hash::{Hash, Hasher};

/// The resource a batch of signals was emitted under.
///
/// Resources are never merged — this runtime is a destination, not a relay.
/// Resource *identity* is the attribute map alone (see
/// [`Resource::identity`]); `schema_url` and the emitter-reported
/// dropped-attributes count are preserved as data next to it.
#[derive(Clone, Debug)]
pub struct Resource {
    /// The attribute map the emitter attached to the resource.
    pub attributes: Attributes,
    /// The resource-level `schema_url`, preserved as sent; `None` when the
    /// emitter sent none. Metadata: it never participates in identity.
    pub schema_url: Option<String>,
    /// The emitter's `dropped_attributes_count` for this resource.
    /// Emitter-reported loss is data.
    pub dropped_attributes_count: u32,
}

impl PartialEq for Resource {
    /// Attribute-map equality only: the comparison identity is built on.
    /// `schema_url` differences are metadata, not identity differences.
    fn eq(&self, other: &Self) -> bool {
        self.attributes == other.attributes
    }
}

impl Eq for Resource {}

impl Hash for Resource {
    /// Hashes the attribute map only — consistent with [`Resource::eq`].
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.attributes.hash(state);
    }
}

impl Resource {
    /// The attribute map that *is* this resource's identity.
    ///
    /// Two records share a resource exactly when these maps are equal,
    /// regardless of how the emitter batched its exports. Two resources
    /// whose maps are equal but whose `schema_url`s differ are the same
    /// resource carrying two preserved URLs — never a merge.
    #[must_use]
    pub const fn identity(&self) -> &Attributes {
        &self.attributes
    }

    /// Full-field comparison: attributes *and* `schema_url` *and* the
    /// dropped count. Named apart from [`PartialEq`] on purpose — every
    /// identity path uses attribute-map equality; this is for callers that
    /// genuinely mean "same bytes", and it is never the ledger's or the
    /// identity's comparison.
    #[must_use]
    pub fn identical(&self, other: &Self) -> bool {
        self.attributes == other.attributes
            && self.schema_url == other.schema_url
            && self.dropped_attributes_count == other.dropped_attributes_count
    }
}

/// The instrumentation scope a signal was emitted from.
///
/// Scope identity is the whole of (name, version, attributes,
/// `schema_url`): the same name with a different version is a different
/// scope, an empty name is valid, and the scope-level `schema_url`
/// participates in the identity (unlike the resource's). The
/// emitter-reported dropped-attributes count is preserved as data and does
/// not participate in identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct InstrumentationScope {
    /// The scope name, as sent; the empty string is a valid name.
    pub name: String,
    /// The scope version, as sent; `None` when the emitter sent none.
    pub version: Option<String>,
    /// The scope's attribute map, part of the identity.
    pub attributes: Attributes,
    /// The scope-level `schema_url`, preserved as sent and part of the
    /// identity.
    pub schema_url: Option<String>,
    /// The emitter's `dropped_attributes_count` for this scope.
    /// Emitter-reported loss is data.
    pub dropped_attributes_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::values::Value;

    fn map(pairs: &[(&str, i64)]) -> Attributes {
        Attributes::from_pairs(
            pairs
                .iter()
                .map(|(key, value)| ((*key).to_owned(), Value::Int(*value)))
                .collect(),
        )
        .expect("unique keys")
    }

    #[test]
    fn resource_identity_is_the_attribute_map() {
        let first = Resource {
            attributes: map(&[("service.name", 1), ("deployment", 2)]),
            schema_url: None,
            dropped_attributes_count: 0,
        };
        let second = Resource {
            attributes: map(&[("deployment", 2), ("service.name", 1)]),
            schema_url: None,
            dropped_attributes_count: 0,
        };
        assert_eq!(first.identity(), second.identity());
        assert_eq!(first, second);
    }

    #[test]
    fn equal_resource_maps_with_different_schema_urls_are_one_identity() {
        let first = Resource {
            attributes: map(&[("service.name", 1)]),
            schema_url: Some("https://schema/one".to_owned()),
            dropped_attributes_count: 0,
        };
        let second = Resource {
            attributes: map(&[("service.name", 1)]),
            schema_url: Some("https://schema/two".to_owned()),
            dropped_attributes_count: 3,
        };
        assert_eq!(first.identity(), second.identity(), "identity is the map");
        assert_eq!(
            first, second,
            "PartialEq is the identity comparison: metadata is not identity"
        );
        assert!(
            !first.identical(&second),
            "the full-field comparison still distinguishes the preserved bytes"
        );
        assert!(first.identical(&first.clone()));
    }

    #[test]
    fn different_attribute_maps_are_different_identities() {
        let first = Resource {
            attributes: map(&[("service.name", 1)]),
            schema_url: Some("https://schema/one".to_owned()),
            dropped_attributes_count: 0,
        };
        let second = Resource {
            attributes: map(&[("service.name", 2)]),
            schema_url: Some("https://schema/one".to_owned()),
            dropped_attributes_count: 0,
        };
        assert_ne!(first, second, "the map is the identity");
    }

    #[test]
    fn resource_hash_agrees_with_its_equality() {
        use std::collections::hash_map::DefaultHasher;
        let first = Resource {
            attributes: map(&[("service.name", 1)]),
            schema_url: Some("https://schema/one".to_owned()),
            dropped_attributes_count: 0,
        };
        let second = Resource {
            attributes: map(&[("service.name", 1)]),
            schema_url: Some("https://schema/two".to_owned()),
            dropped_attributes_count: 9,
        };
        let hash = |resource: &Resource| {
            let mut hasher = DefaultHasher::new();
            resource.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(hash(&first), hash(&second), "hash follows identity");
    }

    #[test]
    fn the_same_scope_name_with_a_different_version_is_a_different_scope() {
        let base = InstrumentationScope {
            name: "scope".to_owned(),
            version: Some("1.0".to_owned()),
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        };
        let other_version = InstrumentationScope {
            version: Some("2.0".to_owned()),
            ..base.clone()
        };
        assert_ne!(base, other_version);
    }

    #[test]
    fn an_empty_scope_name_is_valid_and_scope_fields_participate_in_identity() {
        let empty_named = InstrumentationScope {
            name: String::new(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        };
        assert!(empty_named.name.is_empty(), "an empty name is a valid name");
        let with_attributes = InstrumentationScope {
            attributes: map(&[("k", 1)]),
            ..empty_named.clone()
        };
        assert_ne!(empty_named, with_attributes, "attributes participate");
        let with_schema = InstrumentationScope {
            schema_url: Some("https://scope-schema".to_owned()),
            ..empty_named.clone()
        };
        assert_ne!(
            empty_named, with_schema,
            "the scope schema_url participates"
        );
    }

    #[test]
    fn scope_dropped_count_is_preserved_and_part_of_scope_identity() {
        let base = InstrumentationScope {
            name: "scope".to_owned(),
            version: None,
            attributes: Attributes::default(),
            schema_url: None,
            dropped_attributes_count: 0,
        };
        let with_loss = InstrumentationScope {
            dropped_attributes_count: 12,
            ..base.clone()
        };
        assert_eq!(with_loss.dropped_attributes_count, 12, "preserved");
        assert_ne!(
            base, with_loss,
            "scope identity is full-field: the emitter-reported loss count              distinguishes two scopes, unlike a resource's (attributes only)"
        );
    }

    #[test]
    fn service_is_only_a_resource_attribute_not_a_typed_field() {
        let resource = Resource {
            attributes: Attributes::from_pairs(vec![(
                "service.name".to_owned(),
                Value::String("checkout".to_owned()),
            )])
            .expect("ok"),
            schema_url: None,
            dropped_attributes_count: 0,
        };
        assert_eq!(
            resource.identity().get("service.name"),
            Some(&Value::String("checkout".to_owned())),
            "service identity is read like any attribute; no typed Service exists"
        );
    }
}
