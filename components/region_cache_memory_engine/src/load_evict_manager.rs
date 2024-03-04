// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::{BTreeMap, BTreeSet};

use engine_traits::CacheRange;
use kvproto::metapb::Region;
use raftstore::coprocessor::RegionInfoProvider;

// TODO: This should be configurable.
const NUM_REGIONS_TO_CACHE: usize = 10;

#[derive(Debug, Clone)]
pub struct Action {
    pub must_cache_ranges: Vec<CacheRange>,
    pub may_evict_ranges: Vec<CacheRange>,
    pub must_evict_ranges: Vec<CacheRange>,
}

pub trait LoadEvictManager {
    fn next_action(&mut self) -> Action;
}

pub struct RegionInfoLoadEvictManager {
    region_info_provider: Box<dyn RegionInfoProvider>,
    previous_top_regions: Option<BTreeMap<u64, Region>>,
}

impl RegionInfoLoadEvictManager {
    fn num_regions_to_cache(&self) -> usize {
        NUM_REGIONS_TO_CACHE // Todo: use callback based on configuration.
    }

    fn previous_top_regions(&self) -> Option<BTreeSet<u64>> {
        self.previous_top_regions
            .as_ref()
            .map(|top_regions_set| top_regions_set.keys().copied().collect::<BTreeSet<u64>>())
    }
}

impl LoadEvictManager for RegionInfoLoadEvictManager {
    fn next_action(&mut self) -> Action {
        let top_regions: Vec<Region> = self
            .region_info_provider
            .get_top_regions(self.num_regions_to_cache())
            .unwrap();

        let (must_cache_ranges, may_evict_ranges, must_evict_ranges) =
            if let Some(previous_top_regions) = self.previous_top_regions() {
                let must_cache_ranges = top_regions
                    .iter()
                    .filter(|region| !previous_top_regions.contains(&region.get_id()))
                    .map(|region| CacheRange {
                        start: region.get_start_key().to_vec(),
                        end: region.get_end_key().to_vec(),
                    })
                    .collect::<Vec<_>>();
                let top_regions = top_regions
                    .clone()
                    .iter()
                    .map(Region::get_id)
                    .collect::<BTreeSet<_>>();
                let may_evict_ranges = previous_top_regions
                    .difference(&top_regions)
                    .into_iter()
                    .map(|id| {
                        let region = self.previous_top_regions
                            .as_ref()
                            .unwrap()
                            .get(id)
                            .unwrap();
                        CacheRange {
                            start: region.get_start_key().to_vec(),
                            end: region.get_end_key().to_vec(),
                        }
                    })
                    .collect::<Vec<_>>();
                (must_cache_ranges, Vec::new(), Vec::new())
            } else {
                let must_cache_ranges = top_regions
                    .clone()
                    .iter()
                    .map(|region| CacheRange {
                        start: region.get_start_key().to_vec(),
                        end: region.get_end_key().to_vec(),
                    })
                    .collect::<Vec<_>>();
                (must_cache_ranges, Vec::new(), Vec::new())
            };
        let previous_top_regions = top_regions
            .iter()
            .map(|region| (region.get_id(), region.clone()))
            .collect::<BTreeMap<_, _>>();
        _ = self.previous_top_regions.insert(previous_top_regions);
        Action {
            must_cache_ranges,
            may_evict_ranges,
            must_evict_ranges,
        }
    }
}

pub struct FixedLoadEvictManager;

impl LoadEvictManager for FixedLoadEvictManager {
    fn next_action(&mut self) -> Action {
        Action {
            must_cache_ranges: Vec::new(),
            may_evict_ranges: Vec::new(),
            must_evict_ranges: Vec::new(),
        }
    }
}
