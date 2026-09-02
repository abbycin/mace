//! User-provided merge operators (runtime-only, never persisted).

use std::sync::Arc;

/// user-provided merge operator for a bucket
///
/// The operator turns a base value plus a chain of merge operands into the logical value:
///
/// ```text
/// apply(base, combine_operands(a, b)) == apply(apply(base, a), b)
/// ```
///
/// `base = None` stands for "key absent or deleted" and belongs to the domain of `apply`.
/// Implementations must be deterministic and must satisfy associativity, commutativity of
/// `combine_operands`, and the base action law above. They must not read time, randomness,
/// external I/O, or mutable process state, and must not produce observable side effects:
/// compaction and split may retry or redo fold attempts.
///
/// Reads combine operands from newest to oldest; compaction combines them from oldest to newest.
/// Commutativity and associativity make the two orders equivalent. The methods are total and
/// must return non-empty results; an empty or oversized result is an engine-detected violation.
/// `None` from `apply` is the only logical deletion. The engine does not invoke the operator during
/// merge admission or catch panics.
///
/// The engine stores only this runtime handle and never persists operator identity. Callers own
/// semantic compatibility across process restarts.
pub trait MergeOperator: Send + Sync {
    /// Combines two operands into one equivalent operand. Must return a non-empty
    /// equivalent operand for every pair the operator accepted.
    fn combine_operands(&self, key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8>;

    /// Applies one operand to a base value. `None` deletes or leaves the key absent;
    /// `Some(value)` must be non-empty.
    fn apply(&self, key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>>;
}

/// Default 64-bit unsigned integer additive merge operator
///
/// Operands and base values are little-endian `u64` deltas: `apply` adds the operand onto the base
/// (zero when the key is absent or deleted), and `combine_operands` adds the two operands. A payload
/// that is not exactly 8 bytes decodes as zero. ("corruption will be treated as 0"); the operator
/// stays total for every input and never returns an empty value.
#[derive(Default, Clone, Copy, Debug)]
pub struct U64AddOperator;

impl U64AddOperator {
    fn decode(raw: &[u8]) -> u64 {
        if raw.len() == size_of::<u64>() {
            u64::from_le_bytes(raw.try_into().expect("exact u64 slice"))
        } else {
            // reference behavior: a malformed payload contributes zero
            log::error!(
                "uint64 value corruption, size: {} > {}",
                raw.len(),
                size_of::<u64>()
            );
            0
        }
    }

    fn add(left: u64, right: u64) -> Vec<u8> {
        left.wrapping_add(right).to_le_bytes().to_vec()
    }
}

impl MergeOperator for U64AddOperator {
    fn combine_operands(&self, _key: &[u8], left: &[u8], right: &[u8]) -> Vec<u8> {
        Self::add(Self::decode(left), Self::decode(right))
    }

    fn apply(&self, _key: &[u8], base: Option<&[u8]>, operand: &[u8]) -> Option<Vec<u8>> {
        let base = base.map(Self::decode).unwrap_or(0);
        Some(Self::add(base, Self::decode(operand)))
    }
}

/// Convenience handle for bucket options that want the default counter algebra.
pub fn u64_add_operator() -> Option<Arc<dyn MergeOperator>> {
    Some(Arc::new(U64AddOperator))
}

#[cfg(test)]
mod tests {
    use super::{MergeOperator, U64AddOperator};

    fn decode(raw: &[u8]) -> u64 {
        u64::from_le_bytes(raw.try_into().unwrap())
    }

    #[test]
    fn u64_add_operator_folds_onto_base_and_absence() {
        let op = U64AddOperator;
        // absent base: operand alone
        let v = op.apply(b"k", None, &5u64.to_le_bytes()).unwrap();
        assert_eq!(decode(&v), 5);
        // existing base: base + operand
        let v = op
            .apply(b"k", Some(&10u64.to_le_bytes()), &5u64.to_le_bytes())
            .unwrap();
        assert_eq!(decode(&v), 15);
        // combine: left + right
        let v = op.combine_operands(b"k", &5u64.to_le_bytes(), &7u64.to_le_bytes());
        assert_eq!(decode(&v), 12);
        // wrapping arithmetic, mirroring the reference operator
        let v = op
            .apply(b"k", Some(&u64::MAX.to_le_bytes()), &2u64.to_le_bytes())
            .unwrap();
        assert_eq!(decode(&v), 1);
    }

    #[test]
    fn u64_add_operator_treats_malformed_payload_as_zero() {
        let op = U64AddOperator;
        // non-8-byte operand contributes zero (reference behavior)
        let v = op
            .apply(b"k", Some(&10u64.to_le_bytes()), b"short")
            .unwrap();
        assert_eq!(decode(&v), 10);
        // malformed base decodes as zero
        let v = op.apply(b"k", Some(b"bad"), &5u64.to_le_bytes()).unwrap();
        assert_eq!(decode(&v), 5);
        // outputs are never empty
        let v = op.apply(b"k", None, &0u64.to_le_bytes()).unwrap();
        assert_eq!(decode(&v), 0);
    }
}
