//! Test-only negative-trait probes.
//!
//! Stable Rust cannot express "is not Clone" as a bound, so the consume-once
//! authority types ([`crate::budget::QueryBudget`], [`crate::budget::BudgetSession`],
//! [`crate::lease::SnapshotLease`]) pin their "cannot be duplicated"
//! contract with a compile-time assertion instead: [`assert_not_impl_any`]
//! fails to compile if any listed trait is implemented (issue #26's
//! mutation probe P3 — a probe that added `#[derive(Clone)]` to
//! `QueryBudget` must break the build, not just pass).
//!
//! The mechanism is the classic ambiguity probe: a trait with a marker
//! parameter earns one blanket impl and one impl per listed trait, each on
//! its own marker. A type implementing any listed trait matches at least
//! two impls, leaving the marker inference ambiguous — a compile error.
//! A type implementing none matches only the blanket impl and compiles.
//! The two impl families never overlap textually (distinct markers), so
//! coherence is happy either way.

macro_rules! assert_not_impl_any {
    ($ty:ty: $t1:path) => {
        const _: fn() = || {
            struct Marker0;
            struct Marker1;
            trait AmbiguousIfImpl<Marker> {
                fn some_item() {}
            }
            impl<T: ?Sized> AmbiguousIfImpl<Marker0> for T {}
            impl<T: ?Sized + $t1> AmbiguousIfImpl<Marker1> for T {}
            let _ = <$ty as AmbiguousIfImpl<_>>::some_item;
        };
    };
    ($ty:ty: $t1:path, $t2:path) => {
        const _: fn() = || {
            struct Marker0;
            struct Marker1;
            struct Marker2;
            trait AmbiguousIfImpl<Marker> {
                fn some_item() {}
            }
            impl<T: ?Sized> AmbiguousIfImpl<Marker0> for T {}
            impl<T: ?Sized + $t1> AmbiguousIfImpl<Marker1> for T {}
            impl<T: ?Sized + $t2> AmbiguousIfImpl<Marker2> for T {}
            let _ = <$ty as AmbiguousIfImpl<_>>::some_item;
        };
    };
    ($ty:ty: $t1:path, $t2:path, $t3:path) => {
        const _: fn() = || {
            struct Marker0;
            struct Marker1;
            struct Marker2;
            struct Marker3;
            trait AmbiguousIfImpl<Marker> {
                fn some_item() {}
            }
            impl<T: ?Sized> AmbiguousIfImpl<Marker0> for T {}
            impl<T: ?Sized + $t1> AmbiguousIfImpl<Marker1> for T {}
            impl<T: ?Sized + $t2> AmbiguousIfImpl<Marker2> for T {}
            impl<T: ?Sized + $t3> AmbiguousIfImpl<Marker3> for T {}
            let _ = <$ty as AmbiguousIfImpl<_>>::some_item;
        };
    };
}

pub(crate) use assert_not_impl_any;

#[cfg(test)]
mod tests {

    /// The probe must NOT fire for a type with none of the listed traits —
    /// the pass control that keeps the probe honest (a probe that always
    /// failed would pin nothing).
    #[test]
    fn the_probe_leaves_a_plain_type_alone() {
        struct Plain;
        assert_not_impl_any!(Plain: Clone, Copy, Default);
    }
}
