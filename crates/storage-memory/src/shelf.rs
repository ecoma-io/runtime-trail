//! One kind's residency shelf: the ordered map the residency order lives
//! in.
//!
//! A shelf is the driver's whole data structure: records keyed by
//! [`AdmissionKey`] (admission time, entity id — the one deterministic
//! order eviction and scans share), plus an entity-id index for direct
//! retrieval. Both index and order map hold the same `Arc`ed payload the
//! caller handed in — never a copy (ADR 0008). Not a public surface; the
//! store drives it.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::Arc;

use runtime_trail_storage::{AdmissionKey, ScanPage};
use runtime_trail_telemetry_model::{Accounted, Admitted, EntityId};

/// The resident records of one kind, ordered by [`AdmissionKey`].
pub(crate) struct Shelf<R> {
    by_key: BTreeMap<AdmissionKey, Arc<R>>,
    by_entity: HashMap<EntityId, AdmissionKey>,
    accounted_bytes: u64,
}

impl<R: Accounted> Shelf<R> {
    pub(crate) fn new() -> Self {
        Self {
            by_key: BTreeMap::new(),
            by_entity: HashMap::new(),
            accounted_bytes: 0,
        }
    }

    /// Admits one record. The caller has already refused duplicates.
    pub(crate) fn insert(&mut self, admitted: Admitted<Arc<R>>) {
        let key = AdmissionKey::new(admitted.admitted_at, admitted.entity);
        self.accounted_bytes = self
            .accounted_bytes
            .saturating_add(u64::try_from(admitted.record.accounted_size()).unwrap_or(u64::MAX));
        self.by_entity.insert(admitted.entity, key);
        self.by_key.insert(key, admitted.record);
    }

    /// Whether an entity id is resident.
    pub(crate) fn contains(&self, entity: EntityId) -> bool {
        self.by_entity.contains_key(&entity)
    }

    pub(crate) fn len(&self) -> usize {
        self.by_key.len()
    }

    /// The accounted bytes this shelf currently holds.
    pub(crate) const fn accounted_bytes(&self) -> u64 {
        self.accounted_bytes
    }

    /// The key of the oldest resident record — the one the retention law
    /// evicts first.
    pub(crate) fn smallest_key(&self) -> Option<AdmissionKey> {
        self.by_key.keys().next().copied()
    }

    /// Removes and returns the oldest resident record. Its accounted bytes
    /// leave the shelf's sum with it.
    pub(crate) fn pop_smallest(&mut self) -> Option<(AdmissionKey, Arc<R>)> {
        let key = *self.by_key.keys().next()?;
        let record = self.by_key.remove(&key)?;
        self.by_entity.remove(&key.entity());
        let size = u64::try_from(record.accounted_size()).unwrap_or(u64::MAX);
        self.accounted_bytes = self.accounted_bytes.saturating_sub(size);
        Some((key, record))
    }

    /// The resident record known by `entity`, shared as stored.
    pub(crate) fn get(&self, entity: EntityId) -> Option<Arc<R>> {
        let key = self.by_entity.get(&entity)?;
        self.by_key.get(key).cloned()
    }

    /// The ordered page: at most `limit` records strictly after `after`,
    /// plus the cursor to continue from — `None` at the end of the resident
    /// set. A `limit` of zero yields an empty page and no cursor.
    pub(crate) fn scan_after(&self, after: Option<AdmissionKey>, limit: usize) -> ScanPage<Arc<R>> {
        if limit == 0 {
            return ScanPage {
                items: Vec::new(),
                cursor: None,
            };
        }
        // Two concrete range branches: the scan is on the retrieval path,
        // and a boxed `dyn Iterator` per call bought nothing but a heap
        // allocation.
        match after {
            Some(after) => Self::page_from(
                self.by_key
                    .range((Bound::Excluded(after), Bound::Unbounded))
                    .map(|(key, record)| (*key, record)),
                limit,
            ),
            None => Self::page_from(
                self.by_key.iter().map(|(key, record)| (*key, record)),
                limit,
            ),
        }
    }

    /// Builds the page from an ordered record iterator: at most `limit`
    /// records, and a cursor when a record follows the page's last.
    fn page_from<'a, I>(records: I, limit: usize) -> ScanPage<Arc<R>>
    where
        R: 'a,
        I: Iterator<Item = (AdmissionKey, &'a Arc<R>)>,
    {
        let mut items = Vec::new();
        let mut last_in_page = None;
        let mut cursor = None;
        for (key, record) in records {
            if items.len() == limit {
                // The page is full and a successor exists: the caller
                // resumes strictly after the page's last record.
                cursor = last_in_page;
                break;
            }
            items.push(Arc::clone(record));
            last_in_page = Some(key);
        }
        ScanPage { items, cursor }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Item(u64);

    impl Accounted for Item {
        fn accounted_size(&self) -> usize {
            usize::try_from(self.0).unwrap_or(usize::MAX)
        }
    }

    fn item(serial: u64, nano: u64, size: u64) -> Admitted<Arc<Item>> {
        use runtime_trail_telemetry_model::AdmissionTime;
        use runtime_trail_telemetry_model::AssignedId;
        use std::num::NonZeroU64;

        let serial = NonZeroU64::new(serial).expect("test serials are nonzero");
        Admitted {
            entity: EntityId::Assigned(AssignedId::from_serial(serial)),
            admitted_at: AdmissionTime::from_unix_nano(nano),
            record: Arc::new(Item(size)),
        }
    }

    fn key_of(admitted: &Admitted<Arc<Item>>) -> AdmissionKey {
        AdmissionKey::new(admitted.admitted_at, admitted.entity)
    }

    #[test]
    fn insertion_orders_by_admission_key_not_arrival() {
        let mut shelf = Shelf::new();
        // Sizes double as arrival-order markers: the scan must yield them
        // in admission order (10, 20, 30), not arrival order (30, 10, 20).
        for admitted in [item(1, 300, 30), item(2, 100, 10), item(3, 200, 20)] {
            shelf.insert(admitted);
        }
        assert_eq!(shelf.len(), 3);
        assert_eq!(shelf.accounted_bytes(), 60);
        assert_eq!(
            shelf.smallest_key(),
            Some(key_of(&item(2, 100, 10))),
            "the oldest admission is smallest, whatever the arrival order"
        );
        let page = shelf.scan_after(None, 10);
        let sizes: Vec<usize> = page
            .items
            .iter()
            .map(|item| item.accounted_size())
            .collect();
        assert_eq!(sizes, vec![10, 20, 30]);
        assert!(page.cursor.is_none(), "one page covered the whole shelf");
        let (evicted, record) = shelf.pop_smallest().expect("a record to evict");
        assert_eq!(record.accounted_size(), 10, "the oldest is evicted first");
        assert_eq!(shelf.accounted_bytes(), 50, "the sum leaves with it");
        assert_eq!(evicted, key_of(&item(2, 100, 10)));
        assert!(!shelf.contains(evicted.entity()));
    }

    #[test]
    fn retrieval_and_removal_go_by_entity_id() {
        let mut shelf = Shelf::new();
        let admitted = item(7, 10, 5);
        let entity = admitted.entity;
        shelf.insert(admitted);
        let stored = shelf.get(entity).expect("resident");
        assert_eq!(stored.accounted_size(), 5);
        assert!(shelf.contains(entity));
        // Removing through the smallest-key path removes the entity too:
        // one record, two indexes, one lifecycle.
        shelf.pop_smallest();
        assert!(shelf.get(entity).is_none(), "evicted is absent");
        assert!(!shelf.contains(entity));
    }

    #[test]
    fn a_scan_page_continues_exactly_after_its_cursor() {
        let mut shelf = Shelf::new();
        for serial in 1..=5_u64 {
            shelf.insert(item(serial, serial * 10, serial));
        }
        let first = shelf.scan_after(None, 2);
        assert_eq!(first.items.len(), 2);
        let cursor = first.cursor.expect("more than one page");
        let second = shelf.scan_after(Some(cursor), 2);
        assert_eq!(second.items.len(), 2);
        let third = shelf.scan_after(second.cursor, 2);
        assert_eq!(third.items.len(), 1);
        assert!(third.cursor.is_none(), "the end of the resident set");
        let empty = shelf.scan_after(None, 0);
        assert!(empty.items.is_empty() && empty.cursor.is_none());
    }
}
