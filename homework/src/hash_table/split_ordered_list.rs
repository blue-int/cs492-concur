//! Split-ordered linked list.

use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{Guard, Owned};
use lockfree::list::{Cursor, List, Node};

use super::growable_array::GrowableArray;
use crate::map::NonblockingMap;

/// Lock-free map from `usize` in range [0, 2^63-1] to `V`.
///
/// NOTE: We don't care about hashing in this homework for simplicity.
#[derive(Debug)]
pub struct SplitOrderedList<V> {
    /// Lock-free list sorted by recursive-split order. Use `None` sentinel node value.
    list: List<usize, Option<V>>,
    /// array of pointers to the buckets
    buckets: GrowableArray<Node<usize, Option<V>>>,
    /// number of buckets
    size: AtomicUsize,
    /// number of items
    count: AtomicUsize,
}

impl<V> Default for SplitOrderedList<V> {
    fn default() -> Self {
        Self {
            list: List::new(),
            buckets: GrowableArray::new(),
            size: AtomicUsize::new(2),
            count: AtomicUsize::new(0),
        }
    }
}

impl<V> SplitOrderedList<V> {
    /// `size` is doubled when `count > size * LOAD_FACTOR`.
    const LOAD_FACTOR: usize = 2;

    /// Creates a new split ordered list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a cursor and moves it to the bucket for the given index.  If the bucket doesn't
    /// exist, recursively initializes the buckets.
    fn lookup_bucket<'s>(
        &'s self,
        size: usize,
        index: usize,
        guard: &'s Guard,
    ) -> Cursor<'s, usize, Option<V>> {
        fn get_parent(my_bucket: usize, size: usize) -> usize {
            let mut parent = size;
            loop {
                parent = parent >> 1;
                if parent <= my_bucket {
                    break;
                }
            }
            parent = my_bucket - parent;
            return parent;
        }
        let atomic = self.buckets.get(index, guard);
        let shared = atomic.load(Ordering::Acquire, guard);
        if shared.is_null() {
            let parent = get_parent(index, size);
            let mut cursor;
            if parent == 0 {
                cursor = self.list.head(guard);
            } else {
                cursor = self.lookup_bucket(size, parent, guard);
            }
            let ckpt = cursor.clone();
            let mut owned = Owned::new(Node::new(index.reverse_bits(), None::<V>));
            loop {
                cursor = ckpt.clone();
                match cursor.find_harris(&index.reverse_bits(), guard) {
                    Ok(true) => break,
                    Ok(false) => (),
                    _ => continue,
                }
                match cursor.insert(owned, guard) {
                    Err(n) => owned = n,
                    Ok(_) => break,
                }
            }
            atomic.store(cursor.curr(), Ordering::Release);
            cursor
        } else {
            unsafe { Cursor::from_raw(atomic, shared.as_raw()) }
        }
    }

    /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
    /// Returns `(size, found, cursor)`
    fn find<'s>(
        &'s self,
        key: &usize,
        guard: &'s Guard,
    ) -> (usize, bool, Cursor<'s, usize, Option<V>>) {
        let size = self.size.load(Ordering::Relaxed);
        let mut cursor = self.lookup_bucket(size, key.clone() % size, guard);
        loop {
            if let Ok(found) = cursor.find_harris(&(key.reverse_bits() | 1), guard) {
                return (size, found, cursor);
            }
        }
    }

    fn assert_valid_key(key: usize) {
        assert!(key.leading_zeros() != 0);
    }
}

impl<V> NonblockingMap<usize, V> for SplitOrderedList<V> {
    fn lookup<'a>(&'a self, key: &usize, guard: &'a Guard) -> Option<&'a V> {
        Self::assert_valid_key(*key);
        let (_, found, cursor) = self.find(key, guard);
        if found {
            cursor.lookup().unwrap().as_ref()
        } else {
            None
        }
    }

    fn insert(&self, key: &usize, value: V, guard: &Guard) -> Result<(), V> {
        Self::assert_valid_key(*key);
        let (size, found, mut cursor) = self.find(key, guard);
        let owned = Owned::new(Node::new(key.clone().reverse_bits() | 1, Some(value)));
        if found {
            return Err(owned.into_box().into_value().unwrap());
        }
        match cursor.insert(owned, guard) {
            Ok(()) => {
                let count = self.count.fetch_add(1, Ordering::Relaxed) + 1;
                if count > size * Self::LOAD_FACTOR {
                    self.size.compare_and_swap(size, size * 2, Ordering::Relaxed);
                }
                Ok(())
            }
            Err(owned) => Err(owned.into_box().into_value().unwrap()),
        }
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);
        let (_, found, cursor) = self.find(key, guard);
        if found == false {
            return Err(());
        }
        if let Ok(Some(value)) = cursor.delete(guard) {
            self.count.fetch_sub(1, Ordering::Relaxed);
            Ok(value)
        } else {
            Err(())
        }
    }
}
