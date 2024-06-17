// Copyright C 2024 TiKV Project Authors. Licensed under Apache-2.0.
//
use std::{thread, time::Duration};

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use raftstore::coprocessor::RegionCollector;


fn new_region_collector() -> RegionCollector {
    todo!()
}

fn create_region(c: &mut RegionCollector, region: &Region, role: StateRole) {
}
fn load_regions(c: &mut RegionCollector, regions: &[Region]) {
    for region in regions {
        create_region(c, region, StateRole::Leader);
    }
}

fn gen_regions(num_regions: usize) -> Vec<Region> {
    todo!()
}

fn bench_get_top_regions(c: &mut Criterion) {
    let mut group = c.benchmark_group("bench_get_top_regions");

    for n in 0..18 {
        let num_regions = 2usize.pow(n);
        group.bench_with_input(
            BenchmarkId::new("get_top_regions", num_regions),
            &num_regions,
            |b, num_regions| {
                let regions = gen_regions(num_regions);
                let mut c: RegionCollector = new_region_collector();
                     
                b.iter(|| black_box(thread::sleep(Duration::from_millis(1))));
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_get_top_regions);
criterion_main!(benches);
