use super::{ActorRef, PortRef};
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Clone, Debug)]
pub(crate) struct Sub {
    pub(crate) owner: ActorRef,
    pub(crate) source: ActorRef,
    pub(crate) target: ActorRef,
    pub(crate) source_port: Option<u16>,
    pub(crate) target_port: Option<u16>,
}

pub(crate) struct RouteTable {
    entries: BTreeMap<u64, Sub>,
    by_source: HashMap<(ActorRef, Option<u16>), BTreeSet<u64>>,
    by_source_actor: HashMap<ActorRef, BTreeSet<u64>>,
    by_owner: HashMap<ActorRef, BTreeSet<u64>>,
    by_target: HashMap<ActorRef, BTreeSet<u64>>,
}

impl RouteTable {
    pub(crate) fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            by_source: HashMap::new(),
            by_source_actor: HashMap::new(),
            by_owner: HashMap::new(),
            by_target: HashMap::new(),
        }
    }
    pub(crate) fn active_len(&self) -> usize {
        self.entries.len()
    }
    pub(crate) fn get(&self, id: &u64) -> Option<&Sub> {
        self.entries.get(id)
    }
    pub(crate) fn contains_active(&self, id: u64) -> bool {
        self.entries.contains_key(&id)
    }
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&u64, &Sub)> {
        self.entries.iter()
    }
    pub(crate) fn source_ids(&self, source: ActorRef, port: Option<u16>) -> Vec<u64> {
        self.by_source
            .get(&(source, port))
            .into_iter()
            .flat_map(|ids| ids.iter().copied())
            .collect()
    }
    pub(crate) fn source_count(&self, source: ActorRef, port: Option<u16>) -> usize {
        self.by_source.get(&(source, port)).map_or(0, BTreeSet::len)
    }
    pub(crate) fn source_actor_ids(&self, source: ActorRef) -> Vec<u64> {
        self.by_source_actor
            .get(&source)
            .into_iter()
            .flat_map(|ids| ids.iter().copied())
            .collect()
    }
    pub(crate) fn target_ids(&self, target: ActorRef) -> Vec<u64> {
        self.by_target
            .get(&target)
            .into_iter()
            .flat_map(|ids| ids.iter().copied())
            .collect()
    }
    pub(crate) fn actor_route_ids(&self, actor: ActorRef) -> Vec<u64> {
        let mut ids = BTreeSet::new();
        if let Some(values) = self.by_owner.get(&actor) {
            ids.extend(values);
        }
        if let Some(values) = self.by_source_actor.get(&actor) {
            ids.extend(values);
        }
        if let Some(values) = self.by_target.get(&actor) {
            ids.extend(values);
        }
        ids.into_iter().collect()
    }
    pub(crate) fn insert(&mut self, id: u64, route: Sub) {
        debug_assert!(!self.entries.contains_key(&id));
        self.by_source
            .entry((route.source, route.source_port))
            .or_default()
            .insert(id);
        self.by_source_actor
            .entry(route.source)
            .or_default()
            .insert(id);
        self.by_owner.entry(route.owner).or_default().insert(id);
        self.by_target.entry(route.target).or_default().insert(id);
        self.entries.insert(id, route);
    }
    pub(crate) fn remove(&mut self, id: &u64) -> Option<Sub> {
        let route = self.entries.remove(id)?;
        Self::remove_index(&mut self.by_source, &(route.source, route.source_port), id);
        Self::remove_index(&mut self.by_source_actor, &route.source, id);
        Self::remove_index(&mut self.by_owner, &route.owner, id);
        Self::remove_index(&mut self.by_target, &route.target, id);
        Some(route)
    }
    fn remove_index<K: std::hash::Hash + Eq>(
        index: &mut HashMap<K, BTreeSet<u64>>,
        key: &K,
        id: &u64,
    ) {
        if let Some(ids) = index.get_mut(key) {
            ids.remove(id);
            if ids.is_empty() {
                index.remove(key);
            }
        }
    }
    pub(crate) fn source_target_exists(&self, source: ActorRef, target: ActorRef) -> bool {
        self.source_actor_ids(source).into_iter().any(|id| {
            self.entries
                .get(&id)
                .is_some_and(|route| route.target == target)
        })
    }
    pub(crate) fn typed_duplicate(&self, source: PortRef, target: PortRef) -> bool {
        self.source_ids(source.owner, Some(source.index))
            .into_iter()
            .any(|id| {
                self.entries
                    .get(&id)
                    .is_some_and(|route| route.target == target.owner)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actor(slot: u32) -> ActorRef {
        ActorRef {
            world: 1,
            slot,
            generation: 1,
        }
    }

    fn assert_consistent(routes: &RouteTable) {
        let size = routes.entries.len();
        assert_eq!(
            routes.by_source.values().map(BTreeSet::len).sum::<usize>(),
            size
        );
        for index in [&routes.by_source_actor, &routes.by_owner, &routes.by_target] {
            assert_eq!(index.values().map(BTreeSet::len).sum::<usize>(), size);
            assert!(index.values().all(|ids| !ids.is_empty()));
        }
        assert!(routes.by_source.values().all(|ids| !ids.is_empty()));
        for (id, route) in &routes.entries {
            assert!(routes.by_source[&(route.source, route.source_port)].contains(id));
            assert!(routes.by_source_actor[&route.source].contains(id));
            assert!(routes.by_owner[&route.owner].contains(id));
            assert!(routes.by_target[&route.target].contains(id));
        }
    }

    #[test]
    fn indexes_remain_consistent_through_insert_remove_reuse() {
        let source = actor(1);
        let target = actor(2);
        let mut routes = RouteTable::new();
        for id in 1..=64 {
            routes.insert(
                id,
                Sub {
                    owner: source,
                    source,
                    target,
                    source_port: Some((id % 4) as u16),
                    target_port: Some((id % 2) as u16),
                },
            );
            assert_consistent(&routes);
        }
        assert_eq!(routes.active_len(), 64);
        assert_eq!(routes.source_actor_ids(source).len(), 64);
        assert_eq!(routes.target_ids(target).len(), 64);
        assert!(routes.by_source.values().all(|ids| ids.len() <= 64));
        assert!(routes.by_owner.values().all(|ids| ids.len() <= 64));
        assert!(routes.by_target.values().all(|ids| ids.len() <= 64));
        for id in 1..=64 {
            assert!(routes.remove(&id).is_some());
            assert_consistent(&routes);
        }
        assert_eq!(routes.active_len(), 0);
        assert!(routes.entries.is_empty());
        assert!(routes.by_source.is_empty());
        assert!(routes.by_source_actor.is_empty());
        assert!(routes.by_owner.is_empty());
        assert!(routes.by_target.is_empty());
        routes.insert(
            65,
            Sub {
                owner: source,
                source,
                target,
                source_port: None,
                target_port: None,
            },
        );
        assert_eq!(routes.source_ids(source, None), vec![65]);
        assert_consistent(&routes);
    }

    #[test]
    fn unrelated_routes_do_not_enter_source_or_owner_candidate_sets() {
        let mut routes = RouteTable::new();
        for id in 1..=256 {
            routes.insert(
                id,
                Sub {
                    owner: actor(10),
                    source: actor(11),
                    target: actor(12),
                    source_port: Some(1),
                    target_port: Some(0),
                },
            );
        }
        routes.insert(
            257,
            Sub {
                owner: actor(1),
                source: actor(2),
                target: actor(3),
                source_port: Some(1),
                target_port: Some(0),
            },
        );
        routes.insert(
            258,
            Sub {
                owner: actor(2),
                source: actor(2),
                target: actor(2),
                source_port: None,
                target_port: None,
            },
        );
        assert_eq!(routes.source_ids(actor(2), Some(1)), vec![257]);
        assert_eq!(routes.target_ids(actor(3)), vec![257]);
        assert_eq!(routes.actor_route_ids(actor(1)), vec![257]);
        assert_eq!(routes.actor_route_ids(actor(2)), vec![257, 258]);
        assert_consistent(&routes);
        for id in routes.actor_route_ids(actor(2)) {
            routes.remove(&id).unwrap();
        }
        assert_eq!(routes.active_len(), 256);
        assert_consistent(&routes);
        let replacement = ActorRef {
            generation: 2,
            ..actor(2)
        };
        assert!(routes.actor_route_ids(replacement).is_empty());
    }
}
