#![no_main]
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::merge_sort::MergeSortExec;
use datafusion::physical_plan::ExecutionPlan;
use itertools::Itertools;
use libfuzzer_sys::arbitrary;
use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::option::Option::None;
use std::sync::Arc;

fuzz_target!(|data: MergeSortInputs| { fuzz_merge_sort(data) });

fn fuzz_merge_sort(data: MergeSortInputs) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            let batches = datafusion::physical_plan::collect(data.exec)
                .await
                .unwrap();
            let mut actual = Vec::new();
            for b in batches {
                actual.extend_from_slice(
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values(),
                );
            }

            assert_eq!(data.expected_output, actual);
        })
}

fn create_merge_exec(data: &Vec<MergeSortPartition>) -> MergeSortExec {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
    let mut partitions = Vec::with_capacity(data.len());
    for p in data {
        let mut batches = Vec::with_capacity(p.batches.len());
        for b in &p.batches {
            batches.push(if b.is_empty() {
                RecordBatch::new_empty(schema.clone())
            } else {
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(b.clone()))])
                    .unwrap()
            })
        }
        partitions.push(batches)
    }
    let input = Arc::new(MemoryExec::try_new(&partitions, schema.clone(), None).unwrap());
    MergeSortExec::try_new(input, vec!["a".to_string()]).unwrap()
}

struct MergeSortInputs {
    partitions: Vec<MergeSortPartition>,
    exec: Arc<dyn ExecutionPlan>,
    expected_output: Vec<i64>,
}

impl Debug for MergeSortInputs {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.partitions.fmt(f)
    }
}

impl MergeSortInputs {
    fn from_source(partitions: Vec<MergeSortPartition>) -> MergeSortInputs {
        let exec = Arc::new(create_merge_exec(&partitions));
        let mut expected_output = partitions
            .iter()
            .map(|i| i.batches.as_slice())
            .flatten()
            .flatten()
            .cloned()
            .collect_vec();
        expected_output.sort_unstable();
        MergeSortInputs {
            partitions,
            exec,
            expected_output,
        }
    }
}

impl<'a> Arbitrary<'a> for MergeSortInputs {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self::from_source(Arbitrary::arbitrary(u)?))
    }

    fn arbitrary_take_rest(u: Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(Self::from_source(Arbitrary::arbitrary_take_rest(u)?))
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        <Vec<MergeSortPartition> as Arbitrary>::size_hint(depth)
    }
}

#[derive(Debug)]
struct MergeSortPartition {
    batches: Vec<Vec<i64>>,
}
type ArbSource = (SortedInts, Vec<usize>);
impl MergeSortPartition {
    fn from_source((ints, sizes): ArbSource) -> Self {
        let mut batches = Vec::with_capacity(sizes.len());
        let mut slice = ints.values.as_slice();
        for &s in sizes.iter().chain(&[usize::MAX]) {
            let (current, next) = slice.split_at(s.min(slice.len()));
            batches.push(current.to_vec());
            slice = next;
        }
        MergeSortPartition { batches }
    }
}

impl<'a> Arbitrary<'a> for MergeSortPartition {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(MergeSortPartition::from_source(ArbSource::arbitrary(u)?))
    }

    fn arbitrary_take_rest(u: Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(MergeSortPartition::from_source(
            ArbSource::arbitrary_take_rest(u)?,
        ))
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        ArbSource::size_hint(depth)
    }
}

#[derive(Debug)]
struct SortedInts {
    values: Vec<i64>,
}
impl SortedInts {
    fn sort_and_create(mut values: Vec<i64>) -> SortedInts {
        values.sort_unstable();
        SortedInts { values }
    }
}
impl<'a> Arbitrary<'a> for SortedInts {
    fn arbitrary(u: &mut Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(SortedInts::sort_and_create(Arbitrary::arbitrary(u)?))
    }

    fn arbitrary_take_rest(u: Unstructured<'a>) -> arbitrary::Result<Self> {
        Ok(SortedInts::sort_and_create(Arbitrary::arbitrary_take_rest(
            u,
        )?))
    }

    fn size_hint(depth: usize) -> (usize, Option<usize>) {
        <Vec<i64> as Arbitrary>::size_hint(depth)
    }
}
