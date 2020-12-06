//! Growable array.

use core::fmt::Debug;
use core::marker::PhantomData;
use core::mem;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Pointer, Shared};

/// Growable array of `Atomic<T>`.
///
/// This is more complete version of the dynamic sized array from the paper. In the paper, the
/// segment table is an array of arrays (segments) of pointers to the elements. In this
/// implementation, a segment contains the pointers to the elements **or other segments**. In other
/// words, it is a tree that has segments as internal nodes.
///
/// # Example run
///
/// Suppose `SEGMENT_LOGSIZE = 3` (segment size 8).
///
/// When a new `GrowableArray` is created, `root` is initialized with `Atomic::null()`.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
/// ```
///
/// When you store element `cat` at the index `0b001`, it first initializes a segment.
///
/// ```text
///
///                          +----+
///                          |root|
///                          +----+
///                            | height: 1
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                           |
///                                           v
///                                         +---+
///                                         |cat|
///                                         +---+
/// ```
///
/// When you store `fox` at `0b111011`, it is clear that there is no room for indices larger than
/// `0b111`. So it first allocates another segment for upper 3 bits and moves the previous root
/// segment (`0b000XXX` segment) under the `0b000XXX` branch of the the newly allocated segment.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                               |
///                                               v
///                                      +---+---+---+---+---+---+---+---+
///                                      |111|110|101|100|011|010|001|000|
///                                      +---+---+---+---+---+---+---+---+
///                                                                |
///                                                                v
///                                                              +---+
///                                                              |cat|
///                                                              +---+
/// ```
///
/// And then, it allocates another segment for `0b111XXX` indices.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                                            |
///                   v                                            v
///                 +---+                                        +---+
///                 |fox|                                        |cat|
///                 +---+                                        +---+
/// ```
///
/// Finally, when you store `owl` at `0b000110`, it traverses through the `0b000XXX` branch of the
/// level-1 segment and arrives at its 0b110` leaf.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                        |                   |
///                   v                        v                   v
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// When the array is dropped, only the segments are dropped and the **elements must not be
/// dropped/deallocated**.
///
/// ```test
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// Instead, it should be handled by the container that the elements actually belong to. For
/// example in `SplitOrderedList`, destruction of elements are handled by `List`.
///
#[derive(Debug)]
pub struct GrowableArray<T> {
    root: Atomic<Segment>,
    _marker: PhantomData<T>,
}

const SEGMENT_LOGSIZE: usize = 10;

struct Segment {
    /// `AtomicUsize` here means `Atomic<T>` or `Atomic<Segment>`.
    inner: [AtomicUsize; 1 << SEGMENT_LOGSIZE],
}

impl Segment {
    fn new() -> Self {
        Self {
            inner: unsafe { mem::zeroed() },
        }
    }
}

impl Deref for Segment {
    type Target = [AtomicUsize; 1 << SEGMENT_LOGSIZE];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Segment {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Debug for Segment {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Segment")
    }
}

impl<T> Drop for GrowableArray<T> {
    /// Deallocate segments, but not the individual elements.
    fn drop(&mut self) {
        println!("drop!")
    }
}

impl<T> Default for GrowableArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GrowableArray<T> {
    /// Create a new growable array.
    pub fn new() -> Self {
        Self {
            root: Atomic::null(),
            _marker: PhantomData,
        }
    }

    /// Returns the reference to the `Atomic` pointer at `index`. Allocates new segments if
    /// necessary.
    pub fn get(&self, mut index: usize, guard: &Guard) -> &Atomic<T> {
        let root_ptr = &self.root;
        let mut shared = Shared::null();
        let mut height = 1;
        let mut max_bits;
        let index_bits = (64 - index.leading_zeros()) as usize;
        loop {
            let seg = Segment::new();
            seg[0].store(Shared::into_usize(shared), Ordering::Relaxed);
            let seg = Owned::new(seg).with_tag(height);
            shared = match root_ptr.compare_and_set(shared, seg, Ordering::AcqRel, guard) {
                Ok(n) => n,
                Err(e) => e.current
            };
            height = shared.tag();
            max_bits = SEGMENT_LOGSIZE * height;
            if max_bits >= index_bits {
                break;
            }
            height += 1;
        }
        loop {
            unsafe {
                let seg_ref = shared.deref();
                index = index << (64 - max_bits) >> (64 - max_bits);
                height -= 1;
                max_bits -= SEGMENT_LOGSIZE;
                let seg_index = index >> max_bits;
                if height > 0 {
                    let raw_ref = seg_ref.get_unchecked(seg_index);
                    let raw_size = raw_ref.load(Ordering::Acquire);
                    shared = Shared::from_usize(raw_size) as Shared<Segment>;
                    if shared.is_null() {
                        shared = Owned::new(Segment::new()).into_shared(guard);
                        let new_size = Shared::into_usize(shared);
                        raw_ref.store(new_size, Ordering::Release);
                    }
                } else {
                    let ptr = &*seg_ref.get_unchecked(seg_index) as *const _ as *const Atomic<T>;
                    return ptr.as_ref().unwrap()
                }
            }
        }
    }
}
