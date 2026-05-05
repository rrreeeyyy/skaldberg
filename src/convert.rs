//! Arrow `RecordBatch` → JSON row converter.
//!
//! arrow_json's `record_batches_to_json_rows` flattens MAP cells into a flat
//! object with *every* observed key — including keys that don't exist on a
//! given row, which appear as nulls. That's surprising for a TSDB where we
//! want each row's labels to be exactly the labels of that one series.
//!
//! This module walks the Arrow arrays directly and emits clean JSON:
//!   - MAP<K,V>     → JSON object {k: v, ...}, only the keys that exist for that row
//!   - LIST<T>      → JSON array
//!   - STRUCT       → JSON object
//!   - Timestamp(*) → ISO-8601 string in UTC
//!   - Date32       → "YYYY-MM-DD"
//!   - NaN/Inf f64  → "NaN" / "Infinity" / "-Infinity" strings (JSON has no native repr)

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeStringArray, ListArray,
    MapArray, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use serde_json::{Map, Number, Value};

pub fn record_batches_to_json_rows(batches: &[&RecordBatch]) -> Vec<Map<String, Value>> {
    let mut rows = Vec::new();
    for batch in batches {
        let schema = batch.schema();
        let cols: Vec<(String, ArrayRef)> = (0..batch.num_columns())
            .map(|i| (schema.field(i).name().clone(), batch.column(i).clone()))
            .collect();
        for r in 0..batch.num_rows() {
            let mut row = Map::with_capacity(cols.len());
            for (name, col) in &cols {
                row.insert(name.clone(), array_cell_to_json(col.as_ref(), r));
            }
            rows.push(row);
        }
    }
    rows
}

fn array_cell_to_json(array: &dyn Array, idx: usize) -> Value {
    if array.is_null(idx) {
        return Value::Null;
    }
    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            Value::Bool(a.value(idx))
        }
        DataType::Int8 => num_i64(array.as_any().downcast_ref::<Int8Array>().unwrap().value(idx) as i64),
        DataType::Int16 => num_i64(array.as_any().downcast_ref::<Int16Array>().unwrap().value(idx) as i64),
        DataType::Int32 => num_i64(array.as_any().downcast_ref::<Int32Array>().unwrap().value(idx) as i64),
        DataType::Int64 => num_i64(array.as_any().downcast_ref::<Int64Array>().unwrap().value(idx)),
        DataType::UInt8 => num_u64(array.as_any().downcast_ref::<UInt8Array>().unwrap().value(idx) as u64),
        DataType::UInt16 => num_u64(array.as_any().downcast_ref::<UInt16Array>().unwrap().value(idx) as u64),
        DataType::UInt32 => num_u64(array.as_any().downcast_ref::<UInt32Array>().unwrap().value(idx) as u64),
        DataType::UInt64 => num_u64(array.as_any().downcast_ref::<UInt64Array>().unwrap().value(idx)),
        DataType::Float32 => num_f64(array.as_any().downcast_ref::<Float32Array>().unwrap().value(idx) as f64),
        DataType::Float64 => num_f64(array.as_any().downcast_ref::<Float64Array>().unwrap().value(idx)),
        DataType::Utf8 => Value::String(
            array.as_any().downcast_ref::<StringArray>().unwrap().value(idx).to_string(),
        ),
        DataType::LargeUtf8 => Value::String(
            array.as_any().downcast_ref::<LargeStringArray>().unwrap().value(idx).to_string(),
        ),
        DataType::Timestamp(unit, _) => {
            let (s, ns) = match unit {
                TimeUnit::Second => {
                    let v = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap().value(idx);
                    (v, 0u32)
                }
                TimeUnit::Millisecond => {
                    let v = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value(idx);
                    (v / 1_000, ((v.rem_euclid(1_000)) as u32) * 1_000_000)
                }
                TimeUnit::Microsecond => {
                    let v = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(idx);
                    (v / 1_000_000, ((v.rem_euclid(1_000_000)) as u32) * 1_000)
                }
                TimeUnit::Nanosecond => {
                    let v = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(idx);
                    (v / 1_000_000_000, (v.rem_euclid(1_000_000_000)) as u32)
                }
            };
            chrono::DateTime::from_timestamp(s, ns)
                .map(|dt| Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()))
                .unwrap_or_else(|| Value::String(format!("ts_oor:{}", s)))
        }
        DataType::Date32 => {
            let d = array.as_any().downcast_ref::<Date32Array>().unwrap().value(idx);
            chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(d as i64))
                .map(|nd| Value::String(nd.format("%Y-%m-%d").to_string()))
                .unwrap_or_else(|| Value::String(format!("date32_oor:{}", d)))
        }
        DataType::Map(_, _) => {
            let a = array.as_any().downcast_ref::<MapArray>().unwrap();
            let offsets = a.value_offsets();
            let start = offsets[idx] as usize;
            let end = offsets[idx + 1] as usize;
            let keys = a.keys();
            let values = a.values();
            let mut obj = Map::new();
            for i in start..end {
                if keys.is_null(i) {
                    continue;
                }
                let key = match keys.data_type() {
                    DataType::Utf8 => keys
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(i)
                        .to_string(),
                    DataType::LargeUtf8 => keys
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .unwrap()
                        .value(i)
                        .to_string(),
                    _ => match array_cell_to_json(keys.as_ref(), i) {
                        Value::String(s) => s,
                        other => other.to_string(),
                    },
                };
                obj.insert(key, array_cell_to_json(values.as_ref(), i));
            }
            Value::Object(obj)
        }
        DataType::List(_) => {
            let a = array.as_any().downcast_ref::<ListArray>().unwrap();
            let offsets = a.value_offsets();
            let start = offsets[idx] as usize;
            let end = offsets[idx + 1] as usize;
            let inner = a.values();
            let mut arr = Vec::with_capacity(end - start);
            for i in start..end {
                arr.push(array_cell_to_json(inner.as_ref(), i));
            }
            Value::Array(arr)
        }
        DataType::Struct(fields) => {
            let a = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut obj = Map::with_capacity(fields.len());
            for (i, f) in fields.iter().enumerate() {
                obj.insert(f.name().clone(), array_cell_to_json(a.column(i).as_ref(), idx));
            }
            Value::Object(obj)
        }
        other => Value::String(format!("unsupported:{:?}", other)),
    }
}

fn num_i64(v: i64) -> Value {
    Value::Number(Number::from(v))
}

fn num_u64(v: u64) -> Value {
    Value::Number(Number::from(v))
}

fn num_f64(v: f64) -> Value {
    if v.is_finite() {
        Number::from_f64(v).map(Value::Number).unwrap_or(Value::Null)
    } else if v.is_nan() {
        Value::String("NaN".into())
    } else if v > 0.0 {
        Value::String("Infinity".into())
    } else {
        Value::String("-Infinity".into())
    }
}
