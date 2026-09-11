//! Resource and scope identity.
//!
//! `docs/architecture/telemetry-model.md`, "Resource and scope identity": a
//! resource is an attribute map and two records share a resource exactly
//! when their attribute maps are equal; a scope is identified by
//! (name, version, attributes) and its schema URL participates in that
//! identity; `schema_url` exists at two levels and both are preserved;
//! "service" is not a typed model field — it is the resource attribute
//! `service.name`.

use crate::values::Attributes;

/// The resource a batch of signals was emitted under.
///
/// Resources are never merged — this runtime is a destination, not a relay.
/// Resource *identity* is the attribute map alone (see
/// [`Resource::identity`]); `schema_url` is preserved as data next to it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Resource {
    /// The attribute map the emitter attached to the resource.
    pub attributes: Attributes,
    /// The resource-level `schema_url`, preserved as sent; `None` when the
    /// emitter sent none.
    pub schema_url: Option<String>,
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
}

/// The instrumentation scope a signal was emitted from.
///
/// Scope identity is the whole of (name, version, attributes,
/// `schema_url`): the same name with a different version is a different
/// scope, an empty name is valid, and the scope-level `schema_url`
/// participates in the identity (unlike the resource's).
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
    }

    #[test]
    fn resource_identity_is_the_attribute_map() {
        let first = Resource {
            attributes: map(&[("service.name", 1), ("deployment", 2)]),
            schema_url: None,
        };
        let second = Resource {
            attributes: map(&[("deployment", 2), ("service.name", 1)]),
            schema_url: None,
        };
        assert_eq!(first.identity(), second.identity());
        assert_eq!(first, second);
    }

    #[test]
    fn equal_resource_maps_with_different_schema_urls_share_identity_still_as_data() {
        let first = Resource {
            attributes: map(&[("service.name", 1)]),
            schema_url: Some("https://schema/one".to_owned()),
        };
        let second = Resource {
            attributes: map(&[("service.name", 1)]),
            schema_url: Some("https://schema/two".to_owned()),
        };
        assert_eq!(first.identity(), second.identity(), "identity is the map");
        assert_ne!(first, second, "both schema_urls are preserved data");
    }

    #[test]
    fn the_same_scope_name_with_a_different_version_is_a_different_scope() {
        let base = InstrumentationScope {
            name: "scope".to_owned(),
            version: Some("1.0".to_owned()),
            attributes: Attributes::default(),
            schema_url: None,
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
    fn service_is_only_a_resource_attribute_not_a_typed_field() {
        let resource = Resource {
            attributes: Attributes::from_pairs(vec![(
                "service.name".to_owned(),
                Value::String("checkout".to_owned()),
            )]),
            schema_url: None,
        };
        assert_eq!(
            resource.identity().get("service.name"),
            Some(&Value::String("checkout".to_owned())),
            "service identity is read like any attribute; no typed Service exists"
        );
    }
}
