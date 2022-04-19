#[macro_export]
macro_rules! make_string_interval_year_month {
    ($array: ident, $row: ident) => {{
        let s = if $array.is_null($row) {
            None
        } else {
            let interval = $array.value($row) as f64;
            let years = (interval / 12_f64).floor();
            let month = interval - (years * 12_f64);

            Some(format!(
                "{} years {} mons 0 days 0 hours 0 mins 0.00 secs",
                years, month,
            ))
        };

        s
    }};
}

#[macro_export]
macro_rules! make_string_interval_day_time {
    ($array: ident, $row: ident) => {{
        let s = if $array.is_null($row) {
            None
        } else {
            let value: u64 = $array.value($row) as u64;

            let days_parts: i32 = ((value & 0xFFFFFFFF00000000) >> 32) as i32;
            let milliseconds_part: i32 = (value & 0xFFFFFFFF) as i32;

            let secs = milliseconds_part / 1000;
            let mins = secs / 60;
            let hours = mins / 60;

            let secs = secs - (mins * 60);
            let mins = mins - (hours * 60);

            Some(format!(
                "0 years 0 mons {} days {} hours {} mins {}.{:02} secs",
                days_parts,
                hours,
                mins,
                secs,
                (milliseconds_part % 1000),
            ))
        };

        s
    }};
}

#[macro_export]
macro_rules! make_string_interval_month_day_nano {
    ($array: ident, $row: ident) => {{
        let s = if $array.is_null($row) {
            None
        } else {
            let value: u128 = $array.value($row) as u128;

            let months_part: i32 = ((value & 0xFFFFFFFF000000000000000000000000) >> 96) as i32;
            let years = months_part / 12_i32;
            let month = months_part - (years * 12_i32);

            let days_part: i32 = ((value & 0xFFFFFFFF0000000000000000) >> 64) as i32;
            let nanoseconds_part: i64 = (value & 0xFFFFFFFFFFFFFFFF) as i64;

            let secs = nanoseconds_part / 1000000000;
            let mins = secs / 60;
            let hours = mins / 60;

            let secs = secs - (mins * 60);
            let mins = mins - (hours * 60);

            Some(format!(
                "{} years {} mons {} days {} hours {} mins {}.{:02} secs",
                years,
                month,
                days_part,
                hours,
                mins,
                secs,
                (nanoseconds_part % 1000000000),
            ))
        };

        s
    }};
}
