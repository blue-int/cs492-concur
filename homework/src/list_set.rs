#![allow(clippy::mutex_atomic)]
use std::cmp;
use std::ptr;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: Mutex<*mut Node<T>>,
}

unsafe impl<T> Send for Node<T> {}
unsafe impl<T> Sync for Node<T> {}

/// Concurrent sorted singly linked list using lock-coupling.
#[derive(Debug)]
pub struct OrderedListSet<T> {
    head: Mutex<*mut Node<T>>,
}

unsafe impl<T> Send for OrderedListSet<T> {}
unsafe impl<T> Sync for OrderedListSet<T> {}

// reference to the `next` field of previous node which points to the current node
struct Cursor<'l, T>(MutexGuard<'l, *mut Node<T>>);

impl<T> Node<T> {
    fn new(data: T, next: *mut Self) -> *mut Self {
        Box::into_raw(Box::new(Self {
            data,
            next: Mutex::new(next),
        }))
    }
}

impl<'l, T: Ord> Cursor<'l, T> {
    /// Move the cursor to the position of key in the sorted list. If the key is found in the list,
    /// return `true`.
    fn find(&mut self, key: &T) -> bool {
        let ptr = *self.0;
        if ptr.is_null() {
            return false;
        }

        let node = unsafe { ptr.as_ref().unwrap() };

        if node.data == *key {
            true
        } else if node.data > *key {
            false
        } else {
            self.0 = node.next.lock().unwrap();
            Self::find(self, key)
        }
    }
}

impl<T> OrderedListSet<T> {
    /// Creates a new list.
    pub fn new() -> Self {
        Self {
            head: Mutex::new(ptr::null_mut()),
        }
    }
}

impl<T: Ord> OrderedListSet<T> {
    fn find(&self, key: &T) -> (bool, Cursor<T>) {
        let mut cursor = Cursor(self.head.lock().unwrap());
        let result = cursor.find(key);
        (result, cursor)
    }

    /// Returns `true` if the set contains the key.
    pub fn contains(&self, key: &T) -> bool {
        let (result, _) = self.find(key);
        result
    }

    /// Insert a key to the set. If the set already has the key, return the provided key in `Err`.
    pub fn insert(&self, key: T) -> Result<(), T> {
        unsafe {
            let mut prev_guard = self.head.lock().unwrap();
            let mut next_guard = prev_guard;
            loop {
                if let Some(node) = (*next_guard).as_ref() {
                    let data = &node.data;
                    if *data > key {
                        *next_guard = Node::new(key, *next_guard);
                        return Ok(());
                    } else if *data < key {
                        prev_guard = next_guard;
                        next_guard = (*(*prev_guard)).next.lock().unwrap();
                    } else {
                        return Err(key);
                    }
                } else {
                    *next_guard = Node::new(key, *next_guard);
                    return Ok(());
                }
            }
        }
    }

    /// Remove the key from the set and return it.
    pub fn remove(&self, key: &T) -> Result<T, ()> {
        unsafe {
            let mut prev_guard = self.head.lock().unwrap();
            let mut next_guard = prev_guard;
            loop {
                if let Some(node) = (*next_guard).as_ref() {
                    let data = &node.data;
                    if *data > *key {
                        return Err(());
                    } else if *data < *key {
                        prev_guard = next_guard;
                        next_guard = (*(*prev_guard)).next.lock().unwrap();
                    } else {
                        let node = Box::from_raw(*next_guard);
                        *next_guard = *node.next.lock().unwrap();
                        return Ok(node.data);
                    }
                } else {
                    return Err(());
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Iter<'l, T>(Option<MutexGuard<'l, *mut Node<T>>>);

impl<T> OrderedListSet<T> {
    /// An iterator visiting all elements.
    pub fn iter(&self) -> Iter<T> {
        Iter(Some(self.head.lock().unwrap()))
    }
}

impl<'l, T> Iterator for Iter<'l, T> {
    type Item = &'l T;

    fn next(&mut self) -> Option<Self::Item> {
        let guard = self.0.as_ref().unwrap();
        if let Some(node) = unsafe { (*guard).as_ref() } {
            let next_guard = node.next.lock().unwrap();
            self.0 = Some(next_guard);
            Some(&node.data)
        } else {
            self.0 = None;
            None
        }
    }
}

impl<T> Drop for OrderedListSet<T> {
    fn drop(&mut self) {
        let mut next = *self.head.get_mut().unwrap();
        while !next.is_null() {
            let node = unsafe { Box::from_raw(next) };
            next = node.next.into_inner().unwrap();
        }
        drop(next);
    }
}

impl<T> Default for OrderedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
