//! Help to serialize rust types into Otel proto types
//!
//! Most of the code in here is wholesale copied from
//! https://github.com/open-telemetry/opentelemetry-rust/tree/main/opentelemetry-otlp/src/transform
//! Issue tracking them have that publicly exposed so we don't need to copy code:
//! https://github.com/open-telemetry/opentelemetry-rust/issues/627

use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn to_nanos(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_nanos() as u64
}

pub(crate) mod metrics {
    use super::to_nanos;
    use opentelemetry::{
        metrics::{MetricsError, Number, NumberKind},
        sdk::{
            export::metrics::{
                Count, ExportKind, ExportKindFor, Histogram as HgTrait, LastValue, Max, Min,
                Points, Record, Sum as SumTrait,
            },
            metrics::aggregators::{
                ArrayAggregator, HistogramAggregator, LastValueAggregator,
                MinMaxSumCountAggregator, SumAggregator,
            },
            InstrumentationLibrary,
        },
        Array, Key, Value,
    };
    use std::{
        cmp::Ordering,
        collections::{BTreeMap, HashMap},
    };
    use temporal_sdk_core_protos::opentelemetry::{
        common::v1::{
            any_value, AnyValue, ArrayValue, InstrumentationLibrary as ProtoIL, KeyValue,
        },
        metrics::v1::{
            metric::Data, number_data_point, AggregationTemporality, Gauge, Histogram,
            HistogramDataPoint, InstrumentationLibraryMetrics, Metric, NumberDataPoint,
            ResourceMetrics, Sum,
        },
        resource::v1::Resource,
    };

    pub type CheckpointedMetrics = (ResourceWrapper, InstrumentationLibrary, Metric);

    #[derive(PartialEq, derive_more::From)]
    pub struct ResourceWrapper(opentelemetry::sdk::Resource);
    impl Eq for ResourceWrapper {}

    impl Ord for ResourceWrapper {
        fn cmp(&self, other: &Self) -> Ordering {
            self.0.len().cmp(&other.0.len())
        }
    }

    impl PartialOrd for ResourceWrapper {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.0.len().cmp(&other.0.len()))
        }
    }

    impl From<ResourceWrapper> for Resource {
        fn from(resource: ResourceWrapper) -> Self {
            Resource {
                attributes: resource
                    .0
                    .into_iter()
                    .map(|(key, value)| KeyValue {
                        key: key.as_str().to_string(),
                        value: Some(Valwrap(value).into()),
                    })
                    .collect::<Vec<KeyValue>>(),
                dropped_attributes_count: 0,
            }
        }
    }

    struct ILWrap(InstrumentationLibrary);
    impl From<ILWrap> for ProtoIL {
        fn from(library: ILWrap) -> Self {
            ProtoIL {
                name: library.0.name.to_string(),
                version: library.0.version.unwrap_or("").to_string(),
            }
        }
    }

    struct KVR<'a>(&'a Key, &'a Value);
    impl From<KVR<'_>> for KeyValue {
        fn from(kv: KVR) -> Self {
            KeyValue {
                key: kv.0.clone().into(),
                value: Some(Valwrap(kv.1.clone()).into()),
            }
        }
    }

    struct Valwrap(Value);
    impl From<Valwrap> for AnyValue {
        fn from(value: Valwrap) -> Self {
            AnyValue {
                value: match value.0 {
                    Value::Bool(val) => Some(any_value::Value::BoolValue(val)),
                    Value::I64(val) => Some(any_value::Value::IntValue(val)),
                    Value::F64(val) => Some(any_value::Value::DoubleValue(val)),
                    Value::String(val) => Some(any_value::Value::StringValue(val.into_owned())),
                    Value::Array(array) => Some(any_value::Value::ArrayValue(match array {
                        Array::Bool(vals) => array_into_proto(vals),
                        Array::I64(vals) => array_into_proto(vals),
                        Array::F64(vals) => array_into_proto(vals),
                        Array::String(vals) => array_into_proto(vals),
                    })),
                },
            }
        }
    }

    fn array_into_proto<T>(vals: Vec<T>) -> ArrayValue
    where
        Value: From<T>,
    {
        let values = vals
            .into_iter()
            .map(|val| AnyValue::from(Valwrap(Value::from(val))))
            .collect();

        ArrayValue { values }
    }

    struct ExportKindWrap(ExportKind);
    impl From<ExportKindWrap> for AggregationTemporality {
        fn from(kind: ExportKindWrap) -> Self {
            match kind.0 {
                ExportKind::Cumulative => AggregationTemporality::Cumulative,
                ExportKind::Delta => AggregationTemporality::Delta,
            }
        }
    }

    struct NumberWithKind<'a>(Number, &'a NumberKind);
    impl<'a> From<NumberWithKind<'a>> for number_data_point::Value {
        fn from(number: NumberWithKind<'a>) -> Self {
            match &number.1 {
                NumberKind::I64 | NumberKind::U64 => {
                    number_data_point::Value::AsInt(number.0.to_i64(number.1))
                }
                NumberKind::F64 => number_data_point::Value::AsDouble(number.0.to_f64(number.1)),
            }
        }
    }

    pub fn record_to_metric(
        record: &Record,
        export_selector: &dyn ExportKindFor,
    ) -> Result<Metric, MetricsError> {
        let descriptor = record.descriptor();
        let aggregator = record.aggregator().ok_or(MetricsError::NoDataCollected)?;
        let attributes = record
            .attributes()
            .iter()
            .map(|kv| KVR(kv.0, kv.1).into())
            .collect::<Vec<KeyValue>>();
        let temporality: AggregationTemporality =
            ExportKindWrap(export_selector.export_kind_for(descriptor)).into();
        let kind = descriptor.number_kind();
        Ok(Metric {
            name: descriptor.name().to_string(),
            description: descriptor
                .description()
                .cloned()
                .unwrap_or_else(|| "".to_string()),
            unit: descriptor.unit().unwrap_or("").to_string(),
            data: {
                if let Some(array) = aggregator.as_any().downcast_ref::<ArrayAggregator>() {
                    if let Ok(points) = array.points() {
                        Some(Data::Gauge(Gauge {
                            data_points: points
                                .into_iter()
                                .map(|val| NumberDataPoint {
                                    attributes: attributes.clone(),
                                    start_time_unix_nano: to_nanos(*record.start_time()),
                                    time_unix_nano: to_nanos(*record.end_time()),
                                    value: Some(NumberWithKind(val, kind).into()),
                                    ..Default::default()
                                })
                                .collect(),
                        }))
                    } else {
                        None
                    }
                } else if let Some(last_value) =
                    aggregator.as_any().downcast_ref::<LastValueAggregator>()
                {
                    Some({
                        let (val, sample_time) = last_value.last_value()?;
                        Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes,
                                start_time_unix_nano: to_nanos(*record.start_time()),
                                time_unix_nano: to_nanos(sample_time),
                                value: Some(NumberWithKind(val, kind).into()),
                                ..Default::default()
                            }],
                        })
                    })
                } else if let Some(sum) = aggregator.as_any().downcast_ref::<SumAggregator>() {
                    Some({
                        let val = sum.sum()?;
                        Data::Sum(Sum {
                            data_points: vec![NumberDataPoint {
                                attributes,
                                start_time_unix_nano: to_nanos(*record.start_time()),
                                time_unix_nano: to_nanos(*record.end_time()),
                                value: Some(NumberWithKind(val, kind).into()),
                                ..Default::default()
                            }],
                            aggregation_temporality: temporality as i32,
                            is_monotonic: descriptor.instrument_kind().monotonic(),
                        })
                    })
                } else if let Some(histogram) =
                    aggregator.as_any().downcast_ref::<HistogramAggregator>()
                {
                    Some({
                        let (sum, count, buckets) =
                            (histogram.sum()?, histogram.count()?, histogram.histogram()?);
                        Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                attributes,
                                start_time_unix_nano: to_nanos(*record.start_time()),
                                time_unix_nano: to_nanos(*record.end_time()),
                                count,
                                sum: sum.to_f64(kind),
                                bucket_counts: buckets
                                    .counts()
                                    .iter()
                                    .cloned()
                                    .map(|c| c as u64)
                                    .collect(),
                                explicit_bounds: buckets.boundaries().clone(),
                                ..Default::default()
                            }],
                            aggregation_temporality: temporality as i32,
                        })
                    })
                } else if let Some(min_max_sum_count) = aggregator
                    .as_any()
                    .downcast_ref::<MinMaxSumCountAggregator>()
                {
                    Some({
                        let (min, max, sum, count) = (
                            min_max_sum_count.min()?,
                            min_max_sum_count.max()?,
                            min_max_sum_count.sum()?,
                            min_max_sum_count.count()?,
                        );
                        let buckets = vec![min.to_u64(kind), max.to_u64(kind)];
                        let bounds = vec![0.0, 100.0];
                        Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                attributes,
                                start_time_unix_nano: to_nanos(*record.start_time()),
                                time_unix_nano: to_nanos(*record.end_time()),
                                count,
                                sum: sum.to_f64(kind),
                                bucket_counts: buckets,
                                explicit_bounds: bounds,
                                ..Default::default()
                            }],
                            aggregation_temporality: temporality as i32,
                        })
                    })
                } else {
                    None
                }
            },
        })
    }

    // Group metrics with resources and instrumentation libraries with resources first,
    // then instrumentation libraries.
    #[allow(clippy::map_entry)] // caused by https://github.com/rust-lang/rust-clippy/issues/4674
    pub fn sink(metrics: Vec<CheckpointedMetrics>) -> Vec<ResourceMetrics> {
        let mut sink_map = BTreeMap::<
            ResourceWrapper,
            HashMap<InstrumentationLibrary, HashMap<String, Metric>>,
        >::new();
        for (resource, instrumentation_library, metric) in metrics {
            if sink_map.contains_key(&resource) {
                // found resource, see if we can find instrumentation library
                sink_map.entry(resource).and_modify(|map| {
                    if map.contains_key(&instrumentation_library) {
                        map.entry(instrumentation_library).and_modify(|map| {
                            if map.contains_key(&metric.name) {
                                map.entry(metric.name.clone())
                                    .and_modify(|base| merge(base, metric));
                            } else {
                                map.insert(metric.name.clone(), metric);
                            }
                        });
                    } else {
                        map.insert(instrumentation_library, {
                            let mut map = HashMap::new();
                            map.insert(metric.name.clone(), metric);
                            map
                        });
                    }
                });
            } else {
                // insert resource -> instrumentation library -> metrics
                sink_map.insert(resource, {
                    let mut map = HashMap::new();
                    map.insert(instrumentation_library, {
                        let mut map = HashMap::new();
                        map.insert(metric.name.clone(), metric);
                        map
                    });
                    map
                });
            }
        }

        // convert resource -> instrumentation library -> [metrics] into proto struct ResourceMetric
        sink_map
            .into_iter()
            .map(|(resource, metric_map)| ResourceMetrics {
                resource: Some(resource.into()),
                instrumentation_library_metrics: metric_map
                    .into_iter()
                    .map(
                        |(instrumentation_library, metrics)| InstrumentationLibraryMetrics {
                            instrumentation_library: Some(ILWrap(instrumentation_library).into()),
                            metrics: metrics
                                .into_iter()
                                .map(|(_k, v)| v)
                                .collect::<Vec<Metric>>(),
                            schema_url: "".to_string(),
                        },
                    )
                    .collect::<Vec<InstrumentationLibraryMetrics>>(),
                schema_url: "".to_string(),
            })
            .collect()
    }

    // if the data points are the compatible, merge, otherwise do nothing
    macro_rules! merge_compatible_type {
        ($base: ident, $other: ident,
            $ (
                $t:path => $($other_t: path),*
            ) ; *) => {
            match &mut $base.data {
                $(
                    Some($t(base_data)) => {
                        match $other.data {
                            $(
                                Some($other_t(other_data)) => {
                                    if other_data.data_points.len() > 0 {
                                        base_data.data_points.extend(other_data.data_points);
                                    }
                                },
                            )*
                            _ => {}
                        }
                    },
                )*
                _ => {}
            }
        };
    }

    // Merge `other` metric proto struct into base by append its data point.
    // If two metric proto don't have the same type or name, do nothing
    pub(crate) fn merge(base: &mut Metric, other: Metric) {
        if base.name != other.name {
            return;
        }
        merge_compatible_type!(base, other,
            Data::Sum => Data::Sum;
            Data::Gauge => Data::Sum, Data::Gauge;
            Data::Histogram => Data::Histogram;
            Data::Summary => Data::Summary
        );
    }
}
