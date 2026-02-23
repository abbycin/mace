use crate::utils::data::Position;
use parking_lot::Mutex;
use std::collections::BTreeMap;

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DepGroupState {
    Open,
    Sealed,
    Durable,
    Published,
    Aborted,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DepGroup {
    pub(crate) id: u64,
    pub(crate) state: DepGroupState,
    pub(crate) min_lsn: Position,
    pub(crate) max_lsn: Position,
    pub(crate) arena_ids: Vec<u64>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DepGroupError {
    NotFound {
        group_id: u64,
    },
    InvalidStateTransition {
        group_id: u64,
        from: DepGroupState,
        to: DepGroupState,
    },
}

#[derive(Debug, Default)]
struct DepGroupInner {
    next_id: u64,
    groups: BTreeMap<u64, DepGroup>,
    pending_min_lsn: Option<Position>,
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub(crate) struct DepGroupManager {
    inner: Mutex<DepGroupInner>,
}

#[allow(dead_code)]
impl DepGroupManager {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn open_group(
        &self,
        min_lsn: Position,
        max_lsn: Position,
        arena_ids: Vec<u64>,
    ) -> u64 {
        self.open_group_with_state(min_lsn, max_lsn, arena_ids, DepGroupState::Open)
    }

    pub(crate) fn open_group_durable(
        &self,
        min_lsn: Position,
        max_lsn: Position,
        arena_ids: Vec<u64>,
    ) -> u64 {
        self.open_group_with_state(min_lsn, max_lsn, arena_ids, DepGroupState::Durable)
    }

    fn open_group_with_state(
        &self,
        min_lsn: Position,
        max_lsn: Position,
        arena_ids: Vec<u64>,
        state: DepGroupState,
    ) -> u64 {
        let mut lk = self.inner.lock();
        let id = lk.next_id;
        lk.next_id = lk.next_id.wrapping_add(1);

        lk.groups.insert(
            id,
            DepGroup {
                id,
                state,
                min_lsn,
                max_lsn,
                arena_ids,
            },
        );
        lk.pending_min_lsn = Self::calc_pending_min_lsn(&lk.groups);
        id
    }

    pub(crate) fn seal_group(&self, group_id: u64) -> Result<(), DepGroupError> {
        self.transition(group_id, DepGroupState::Open, DepGroupState::Sealed)
    }

    pub(crate) fn mark_group_durable(&self, group_id: u64) -> Result<(), DepGroupError> {
        self.transition(group_id, DepGroupState::Sealed, DepGroupState::Durable)
    }

    pub(crate) fn mark_group_published(&self, group_id: u64) -> Result<(), DepGroupError> {
        self.finalize(group_id, DepGroupState::Durable, DepGroupState::Published)
    }

    pub(crate) fn abort_group(&self, group_id: u64) -> Result<(), DepGroupError> {
        let mut lk = self.inner.lock();
        let Some(state) = lk.groups.get(&group_id).map(|x| x.state) else {
            return Err(DepGroupError::NotFound { group_id });
        };

        match state {
            DepGroupState::Open | DepGroupState::Sealed | DepGroupState::Durable => {
                lk.groups.remove(&group_id);
                lk.pending_min_lsn = Self::calc_pending_min_lsn(&lk.groups);
                Ok(())
            }
            DepGroupState::Published => Err(DepGroupError::InvalidStateTransition {
                group_id,
                from: DepGroupState::Published,
                to: DepGroupState::Aborted,
            }),
            DepGroupState::Aborted => Ok(()),
        }
    }

    pub(crate) fn pending_min_lsn(&self) -> Option<Position> {
        self.inner.lock().pending_min_lsn
    }

    pub(crate) fn state(&self, group_id: u64) -> Option<DepGroupState> {
        self.inner.lock().groups.get(&group_id).map(|x| x.state)
    }

    #[allow(dead_code)]
    pub(crate) fn get(&self, group_id: u64) -> Option<DepGroup> {
        self.inner.lock().groups.get(&group_id).cloned()
    }

    fn transition(
        &self,
        group_id: u64,
        expected: DepGroupState,
        next: DepGroupState,
    ) -> Result<(), DepGroupError> {
        let mut lk = self.inner.lock();
        let Some(group) = lk.groups.get_mut(&group_id) else {
            return Err(DepGroupError::NotFound { group_id });
        };

        if group.state == next {
            return Ok(());
        }

        if group.state != expected {
            return Err(DepGroupError::InvalidStateTransition {
                group_id,
                from: group.state,
                to: next,
            });
        }

        group.state = next;
        lk.pending_min_lsn = Self::calc_pending_min_lsn(&lk.groups);
        Ok(())
    }

    fn finalize(
        &self,
        group_id: u64,
        expected: DepGroupState,
        terminal: DepGroupState,
    ) -> Result<(), DepGroupError> {
        let mut lk = self.inner.lock();
        let Some(state) = lk.groups.get(&group_id).map(|x| x.state) else {
            return Err(DepGroupError::NotFound { group_id });
        };

        if state != expected {
            return Err(DepGroupError::InvalidStateTransition {
                group_id,
                from: state,
                to: terminal,
            });
        }

        lk.groups.remove(&group_id);
        lk.pending_min_lsn = Self::calc_pending_min_lsn(&lk.groups);
        Ok(())
    }

    fn calc_pending_min_lsn(groups: &BTreeMap<u64, DepGroup>) -> Option<Position> {
        groups
            .values()
            .filter(|x| {
                matches!(
                    x.state,
                    DepGroupState::Open | DepGroupState::Sealed | DepGroupState::Durable
                )
            })
            .map(|x| x.min_lsn)
            .min()
    }
}

#[cfg(test)]
mod test {
    use super::{DepGroupError, DepGroupManager, DepGroupState};
    use crate::utils::data::Position;

    fn lsn(file_id: u64, offset: u64) -> Position {
        Position { file_id, offset }
    }

    #[test]
    fn dep_group_state_happy_path() {
        let mgr = DepGroupManager::new();
        let group_id = mgr.open_group(lsn(10, 20), lsn(10, 99), vec![1, 2, 3]);
        assert_eq!(mgr.state(group_id), Some(DepGroupState::Open));
        assert_eq!(mgr.pending_min_lsn(), Some(lsn(10, 20)));

        assert_eq!(mgr.seal_group(group_id), Ok(()));
        assert_eq!(mgr.state(group_id), Some(DepGroupState::Sealed));

        assert_eq!(mgr.mark_group_durable(group_id), Ok(()));
        assert_eq!(mgr.state(group_id), Some(DepGroupState::Durable));

        assert_eq!(mgr.mark_group_published(group_id), Ok(()));
        assert_eq!(mgr.state(group_id), None);
        assert_eq!(mgr.pending_min_lsn(), None);
    }

    #[test]
    fn dep_group_rejects_invalid_transition() {
        let mgr = DepGroupManager::new();
        let group_id = mgr.open_group(lsn(1, 1), lsn(1, 8), vec![3]);

        assert_eq!(
            mgr.mark_group_durable(group_id),
            Err(DepGroupError::InvalidStateTransition {
                group_id,
                from: DepGroupState::Open,
                to: DepGroupState::Durable,
            })
        );
        assert_eq!(mgr.state(group_id), Some(DepGroupState::Open));

        assert_eq!(mgr.seal_group(group_id), Ok(()));
        assert_eq!(
            mgr.mark_group_published(group_id),
            Err(DepGroupError::InvalidStateTransition {
                group_id,
                from: DepGroupState::Sealed,
                to: DepGroupState::Published,
            })
        );
        assert_eq!(mgr.state(group_id), Some(DepGroupState::Sealed));
    }

    #[test]
    fn dep_group_abort_updates_pending_min_lsn() {
        let mgr = DepGroupManager::new();
        let g1 = mgr.open_group(lsn(3, 5), lsn(3, 9), vec![10]);
        let g2 = mgr.open_group(lsn(1, 2), lsn(1, 7), vec![11, 12]);

        assert_eq!(mgr.pending_min_lsn(), Some(lsn(1, 2)));
        assert_eq!(mgr.abort_group(g2), Ok(()));
        assert_eq!(mgr.state(g2), None);
        assert_eq!(mgr.pending_min_lsn(), Some(lsn(3, 5)));

        assert_eq!(mgr.seal_group(g1), Ok(()));
        assert_eq!(mgr.mark_group_durable(g1), Ok(()));
        assert_eq!(mgr.pending_min_lsn(), Some(lsn(3, 5)));

        assert_eq!(mgr.mark_group_published(g1), Ok(()));
        assert_eq!(mgr.pending_min_lsn(), None);
    }

    #[test]
    fn dep_group_open_durable_updates_pending_min_lsn() {
        let mgr = DepGroupManager::new();
        let g1 = mgr.open_group_durable(lsn(7, 3), lsn(7, 9), vec![21]);
        let g2 = mgr.open_group_durable(lsn(5, 1), lsn(5, 2), vec![22]);

        assert_eq!(mgr.pending_min_lsn(), Some(lsn(5, 1)));

        assert_eq!(mgr.mark_group_published(g2), Ok(()));
        assert_eq!(mgr.pending_min_lsn(), Some(lsn(7, 3)));

        assert_eq!(mgr.mark_group_published(g1), Ok(()));
        assert_eq!(mgr.pending_min_lsn(), None);
    }
}
