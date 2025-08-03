use std::cmp::Ordering;

// pub(crate) mod chunk;
pub(crate) mod data;
pub(crate) mod header;
pub(crate) mod imtree;
pub(crate) mod node;
pub(crate) mod page;
pub(crate) mod refbox;
pub(crate) mod sst;
pub(crate) mod traits;

type Comparator<T> = fn(&T, &T) -> Ordering;
