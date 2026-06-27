use datafusion::arrow::array::{ArrayBuilder, StringDictionaryBuilder};
use datafusion::arrow::datatypes::{DataType, Int32Type};
use datafusion::error::DataFusionError;
use datafusion::scalar::ScalarValue;

/// Generic code to help implement generic operations on scalars.
/// Callers must [ScalarValue] to use this.
/// See usages for examples.
#[macro_export]
macro_rules! cube_match_scalar {
    ($scalar: expr, $matcher: ident $(, $arg: tt)*) => {{
        use datafusion::arrow::array::*;
        match $scalar {
            ScalarValue::Boolean(v) => ($matcher!($($arg ,)* v, BooleanBuilder)),
            ScalarValue::Float32(v) => ($matcher!($($arg ,)* v, Float32Builder)),
            ScalarValue::Float64(v) => ($matcher!($($arg ,)* v, Float64Builder)),
            ScalarValue::Decimal128(v, _, _) => ($matcher!($($arg ,)* v, Decimal128Builder)),
            ScalarValue::Decimal256(v, _, _) => ($matcher!($($arg ,)* v, Decimal256Builder)),
            ScalarValue::Int8(v) => ($matcher!($($arg ,)* v, Int8Builder)),
            ScalarValue::Int16(v) => ($matcher!($($arg ,)* v, Int16Builder)),
            ScalarValue::Int32(v) => ($matcher!($($arg ,)* v, Int32Builder)),
            ScalarValue::Int64(v) => ($matcher!($($arg ,)* v, Int64Builder)),
            ScalarValue::UInt8(v) => ($matcher!($($arg ,)* v, UInt8Builder)),
            ScalarValue::UInt16(v) => ($matcher!($($arg ,)* v, UInt16Builder)),
            ScalarValue::UInt32(v) => ($matcher!($($arg ,)* v, UInt32Builder)),
            ScalarValue::UInt64(v) => ($matcher!($($arg ,)* v, UInt64Builder)),
            ScalarValue::Utf8(v) => ($matcher!($($arg ,)* v, StringBuilder)),
            ScalarValue::LargeUtf8(v) => ($matcher!($($arg ,)* v, LargeStringBuilder)),
            ScalarValue::Date32(v) => ($matcher!($($arg ,)* v, Date32Builder)),
            ScalarValue::Date64(v) => ($matcher!($($arg ,)* v, Date64Builder)),
            ScalarValue::TimestampMicrosecond(v, tz) => {
                ($matcher!($($arg ,)* v, TimestampMicrosecondBuilder))
            }
            ScalarValue::TimestampNanosecond(v, tz) => {
                ($matcher!($($arg ,)* v, TimestampNanosecondBuilder))
            }
            ScalarValue::TimestampMillisecond(v, tz) => {
                ($matcher!($($arg ,)* v, TimestampMillisecondBuilder))
            }
            ScalarValue::TimestampSecond(v, tz) => ($matcher!($($arg ,)* v, TimestampSecondBuilder)),
            ScalarValue::IntervalYearMonth(v) => ($matcher!($($arg ,)* v, IntervalYearMonthBuilder)),
            ScalarValue::IntervalDayTime(v) => ($matcher!($($arg ,)* v, IntervalDayTimeBuilder)),
            ScalarValue::List(v) => ($matcher!($($arg ,)* v, v.value_type(), ListBuilder)),
            ScalarValue::Binary(v) => ($matcher!($($arg ,)* v, BinaryBuilder)),
            ScalarValue::LargeBinary(v) => ($matcher!($($arg ,)* v, LargeBinaryBuilder)),
            value => {
                // TODO upgrade DF: Handle?  Or trim this down to supported topk accumulator types?  (Or change topk to accumulate using GroupsAccumulators?)
                panic!("Unhandled cube_match_scalar match arm: {:?}", value);
            }
        }
    }};
}

/// Dictionary group keys (CubeStore dictionary encoding produces `Dictionary(Int32, Utf8)`) are not
/// handled by [cube_match_scalar], which works on plain scalar variants. Build/append into a string
/// dictionary builder so the top-k result columns keep the dictionary type of the schema.
fn create_dictionary_builder(key_type: &DataType, value: &ScalarValue) -> Box<dyn ArrayBuilder> {
    match (key_type, value) {
        (DataType::Int32, ScalarValue::Utf8(_)) => {
            Box::new(StringDictionaryBuilder::<Int32Type>::new())
        }
        _ => panic!(
            "Unhandled dictionary topk type: key={:?} value={:?}",
            key_type, value
        ),
    }
}

fn append_dictionary_value(
    b: &mut dyn ArrayBuilder,
    value: &ScalarValue,
) -> Result<(), DataFusionError> {
    let b = b
        .as_any_mut()
        .downcast_mut::<StringDictionaryBuilder<Int32Type>>()
        .expect("expected StringDictionaryBuilder<Int32Type>");
    match value {
        ScalarValue::Utf8(None) => b.append_null(),
        ScalarValue::Utf8(Some(v)) => b.append_value(v),
        other => panic!("Unhandled dictionary topk value: {:?}", other),
    }
    Ok(())
}

#[allow(unused_variables)]
pub fn create_builder(s: &ScalarValue) -> Box<dyn ArrayBuilder> {
    if let ScalarValue::Dictionary(key_type, value) = s {
        return create_dictionary_builder(key_type, value);
    }
    macro_rules! create_list_builder {
        ($v: expr, $inner_data_type: expr, ListBuilder $(, $rest: tt)*) => {{
            panic!("nested lists not supported")
        }};
        ($v: expr, $builder: tt $(, $rest: tt)*) => {{
            Box::new(ListBuilder::new($builder::new()))
        }};
    }
    macro_rules! create_builder {
        ($v: expr, $inner_data_type: expr, ListBuilder $(, $rest: tt)*) => {{
            let dummy =
                ScalarValue::try_from($inner_data_type).expect("unsupported inner list type");
            cube_match_scalar!(dummy, create_list_builder)
        }};
        ($v: expr, Decimal128Builder $(, $rest: tt)*) => {{
            Box::new(Decimal128Builder::new().with_data_type(s.data_type()))
        }};
        ($v: expr, Decimal256Builder $(, $rest: tt)*) => {{
            Box::new(Decimal256Builder::new().with_data_type(s.data_type()))
        }};
        ($v: expr, $builder: tt $(, $rest: tt)*) => {{
            Box::new($builder::new())
        }};
    }
    cube_match_scalar!(s, create_builder)
}

#[allow(unused_variables)]
pub(crate) fn append_value(
    b: &mut dyn ArrayBuilder,
    v: &ScalarValue,
) -> Result<(), DataFusionError> {
    if let ScalarValue::Dictionary(_key_type, value) = v {
        return append_dictionary_value(b, value);
    }
    let b = b.as_any_mut();
    macro_rules! append_list_value {
        ($list: expr, $dummy: expr, $inner_data_type: expr, ListBuilder $(, $rest: tt)*) => {{
            panic!("nested lists not supported")
        }};
        ($list: expr, $dummy: expr, $builder: tt $(, $rest: tt)* ) => {{
            let b = b
                .downcast_mut::<ListBuilder<$builder>>()
                .expect("invalid list builder");
            let vs = $list;
            // `vs` (a GenericListArray in ScalarValue::List) is supposed to have length 1.  That
            // is, its zero'th element and only element is either null or a list `value_to_append`
            // below, with some arbitrary length.
            if vs.len() == vs.null_count() {
                // ^^ ScalarValue::is_null() code duplication.  is_null() claims some code paths
                // might put a list in `ScalarValue::List` that does not have length 1.
                return Ok(b.append(false));
            }
            let values_builder = b.values();
            let value_to_append: ArrayRef = vs.value(0);
            for i in 0..value_to_append.len() {
                append_value(
                    values_builder,
                    &ScalarValue::try_from_array(&value_to_append, i)?,
                )?;
            }
            Ok(b.append(true))
        }};
    }
    macro_rules! append_value {
        ($v: expr, $inner_data_type: expr, ListBuilder $(, $rest: tt)* ) => {{
            let dummy =
                ScalarValue::try_from($inner_data_type).expect("unsupported inner list type");
            cube_match_scalar!(dummy, append_list_value, $v)
        }};
        ($v: expr, StringBuilder $(, $rest: tt)*) => {{
            let b = b
                .downcast_mut::<StringBuilder>()
                .expect("invalid string builder");
            match $v {
                None => Ok(b.append_null()),
                Some(v) => Ok(b.append_value(v)),
            }
        }};
        ($v: expr, LargeStringBuilder $(, $rest: tt)*) => {{
            let b = b
                .downcast_mut::<LargeStringBuilder>()
                .expect("invalid large string builder");
            match $v {
                None => Ok(b.append_null()),
                Some(v) => Ok(b.append_value(v)),
            }
        }};
        ($v: expr, LargeBinaryBuilder $(, $rest: tt)*) => {{
            let b = b
                .downcast_mut::<LargeBinaryBuilder>()
                .expect("invalid large binary builder");
            match $v {
                None => Ok(b.append_null()),
                Some(v) => Ok(b.append_value(v)),
            }
        }};
        ($v: expr, BinaryBuilder $(, $rest: tt)*) => {{
            let b = b
                .downcast_mut::<BinaryBuilder>()
                .expect("invalid binary builder");
            match $v {
                None => Ok(b.append_null()),
                Some(v) => Ok(b.append_value(v)),
            }
        }};
        ($v: expr, $builder: tt $(, $rest: tt)*) => {{
            let b = b.downcast_mut::<$builder>().expect(stringify!($builder));
            match $v {
                None => Ok(b.append_null()),
                Some(v) => Ok(b.append_value(*v)),
            }
        }};
    }
    cube_match_scalar!(v, append_value)
}
