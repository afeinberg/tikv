// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeSet;

use engine_traits::CacheRange;
use raftstore::coprocessor::RegionInfoProvider;

use kvproto::metapb::Region;

// TODO: This should be configurable.
const NUM_REGIONS_TO_CACHE: usize = 10;

#[derive(Debug, Clone)]
pub struct Action {
    pub must_cache_ranges: Vec<CacheRange>,
    pub may_evict_ranges: Vec<CacheRange>,
    pub must_evict_ranges: Vec<CacheRange>,
}

pub trait LoadEvictManager {

    fn next_action(&self) -> Action;
}

pub struct RegionInfoLoadEvictManager {
    region_info_provider: Box<dyn RegionInfoProvider>,
    previous_top_regions: Option<BTreeSet<u64>>,
}

impl LoadEvictManager for RegionInfoLoadEvictManager {
    fn next_action(&self) -> Action {
        let top_regions: Vec<Region> = self.region_info_provider.get_top_regions(NUM_REGIONS_TO_CACHE).unwrap();

        unimplemented!()
    }
}

pub struct FixedLoadEvictManager;

impl LoadEvictManager for FixedLoadEvictManager {

    fn next_action(&self) -> Action {
        Action { must_cache_ranges: Vec::new(), 
            may_evict_ranges: Vec::new(),
            must_evict_ranges: Vec::new()
        }
    }
}