use std::str::FromStr;

use chrono::{DateTime, Datelike, Duration, NaiveDate, TimeZone, Utc, Weekday};

use crate::domain::{WorkCalendar, Workday};
use crate::error::{MambaError, Result};

const MAX_SEARCH_DAYS: usize = 3_660;

pub fn validate(calendar: &WorkCalendar) -> Result<()> {
    if !(-14 * 60..=14 * 60).contains(&calendar.utc_offset_minutes) {
        return Err(MambaError::Validation(
            "calendar UTC offset must be between -14:00 and +14:00".into(),
        ));
    }
    if calendar.working_days.is_empty() {
        return Err(MambaError::Validation(
            "calendar must contain at least one working day".into(),
        ));
    }
    if calendar.day_start_minute >= calendar.day_end_minute || calendar.day_end_minute > 24 * 60 {
        return Err(MambaError::Validation(
            "calendar work window must be within 00:00..24:00 and start before end".into(),
        ));
    }
    Ok(())
}

pub fn next_available(calendar: &WorkCalendar, start: DateTime<Utc>) -> Result<DateTime<Utc>> {
    validate(calendar)?;
    let offset = Duration::minutes(i64::from(calendar.utc_offset_minutes));
    let mut cursor = start;
    for _ in 0..MAX_SEARCH_DAYS {
        let local = cursor + offset;
        let date = local.date_naive();
        if calendar
            .working_days
            .contains(&Workday::from(local.weekday()))
        {
            let work_start = local_minute_to_utc(date, calendar.day_start_minute, offset);
            let work_end = local_minute_to_utc(date, calendar.day_end_minute, offset);
            let candidate = cursor.max(work_start);
            if candidate < work_end {
                if let Some(block) = calendar
                    .time_off
                    .iter()
                    .filter(|block| block.is_active())
                    .filter(|block| block.starts_at <= candidate && block.ends_at > candidate)
                    .min_by_key(|block| block.ends_at)
                {
                    cursor = block.ends_at;
                    continue;
                }
                return Ok(candidate);
            }
        }
        cursor = local_minute_to_utc(
            date.succ_opt()
                .ok_or_else(|| MambaError::Validation("calendar date overflow".into()))?,
            0,
            offset,
        );
    }
    Err(MambaError::Validation(
        "calendar has no availability within ten years".into(),
    ))
}

pub fn add_working_hours(
    calendar: &WorkCalendar,
    start: DateTime<Utc>,
    hours: f64,
) -> Result<DateTime<Utc>> {
    if !hours.is_finite() || hours < 0.0 {
        return Err(MambaError::Validation(
            "working hours must be a finite non-negative number".into(),
        ));
    }
    let mut remaining_ms = (hours * 3_600_000.0).round() as i64;
    let offset = Duration::minutes(i64::from(calendar.utc_offset_minutes));
    let mut cursor = next_available(calendar, start)?;
    if remaining_ms == 0 {
        return Ok(cursor);
    }

    for _ in 0..MAX_SEARCH_DAYS * 4 {
        let local = cursor + offset;
        let work_end = local_minute_to_utc(local.date_naive(), calendar.day_end_minute, offset);
        let next_block = calendar
            .time_off
            .iter()
            .filter(|block| block.is_active())
            .filter(|block| block.starts_at < work_end && block.ends_at > cursor)
            .min_by_key(|block| block.starts_at);
        if let Some(block) = next_block
            && block.starts_at <= cursor
        {
            cursor = next_available(calendar, block.ends_at)?;
            continue;
        }
        let available_until = next_block
            .map(|block| block.starts_at)
            .unwrap_or(work_end)
            .min(work_end);
        let available_ms = available_until
            .signed_duration_since(cursor)
            .num_milliseconds()
            .max(0);
        if remaining_ms <= available_ms {
            return Ok(cursor + Duration::milliseconds(remaining_ms));
        }
        remaining_ms -= available_ms;
        cursor = next_available(
            calendar,
            next_block.map(|block| block.ends_at).unwrap_or(work_end),
        )?;
    }
    Err(MambaError::Validation(
        "working duration exceeds calendar search limit".into(),
    ))
}

pub fn summary(calendar: &WorkCalendar) -> String {
    let days = calendar
        .working_days
        .iter()
        .map(|day| day.short_name())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "UTC{} · {} · {}-{}",
        format_offset(calendar.utc_offset_minutes),
        days,
        format_minute(calendar.day_start_minute),
        format_minute(calendar.day_end_minute)
    )
}

pub fn parse_workdays(value: &str) -> Result<Vec<Workday>> {
    let mut days = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(Workday::from_str)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    days.sort();
    days.dedup();
    if days.is_empty() {
        return Err(MambaError::Validation(
            "working days cannot be empty".into(),
        ));
    }
    Ok(days)
}

impl Workday {
    pub fn short_name(self) -> &'static str {
        match self {
            Self::Monday => "Mon",
            Self::Tuesday => "Tue",
            Self::Wednesday => "Wed",
            Self::Thursday => "Thu",
            Self::Friday => "Fri",
            Self::Saturday => "Sat",
            Self::Sunday => "Sun",
        }
    }
}

impl From<Weekday> for Workday {
    fn from(value: Weekday) -> Self {
        match value {
            Weekday::Mon => Self::Monday,
            Weekday::Tue => Self::Tuesday,
            Weekday::Wed => Self::Wednesday,
            Weekday::Thu => Self::Thursday,
            Weekday::Fri => Self::Friday,
            Weekday::Sat => Self::Saturday,
            Weekday::Sun => Self::Sunday,
        }
    }
}

impl FromStr for Workday {
    type Err = MambaError;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "mon" | "monday" | "周一" => Ok(Self::Monday),
            "tue" | "tuesday" | "周二" => Ok(Self::Tuesday),
            "wed" | "wednesday" | "周三" => Ok(Self::Wednesday),
            "thu" | "thursday" | "周四" => Ok(Self::Thursday),
            "fri" | "friday" | "周五" => Ok(Self::Friday),
            "sat" | "saturday" | "周六" => Ok(Self::Saturday),
            "sun" | "sunday" | "周日" | "周天" => Ok(Self::Sunday),
            _ => Err(MambaError::Validation(format!("unknown workday `{value}`"))),
        }
    }
}

fn local_minute_to_utc(date: NaiveDate, minute: u16, offset: Duration) -> DateTime<Utc> {
    let (date, minute) = if minute == 24 * 60 {
        (date.succ_opt().expect("validated calendar date"), 0)
    } else {
        (date, minute)
    };
    let naive = date
        .and_hms_opt(u32::from(minute / 60), u32::from(minute % 60), 0)
        .expect("validated calendar minute");
    Utc.from_utc_datetime(&naive) - offset
}

fn format_offset(minutes: i32) -> String {
    let sign = if minutes < 0 { '-' } else { '+' };
    let absolute = minutes.unsigned_abs();
    format!("{sign}{:02}:{:02}", absolute / 60, absolute % 60)
}

fn format_minute(minute: u16) -> String {
    format!("{:02}:{:02}", minute / 60, minute % 60)
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::domain::AvailabilityBlock;

    fn weekday_calendar() -> WorkCalendar {
        WorkCalendar {
            principal_id: "H-1".into(),
            utc_offset_minutes: 8 * 60,
            working_days: parse_workdays("mon,tue,wed,thu,fri").unwrap(),
            day_start_minute: 9 * 60,
            day_end_minute: 18 * 60,
            time_off: Vec::new(),
            updated_by: "admin".into(),
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn working_hours_skip_nights_weekends_and_time_off() {
        let mut calendar = weekday_calendar();
        let friday = Utc.with_ymd_and_hms(2026, 7, 17, 8, 0, 0).unwrap();
        let monday = add_working_hours(&calendar, friday, 4.0).unwrap();
        assert_eq!(monday, Utc.with_ymd_and_hms(2026, 7, 20, 3, 0, 0).unwrap());

        calendar.time_off.push(AvailabilityBlock {
            id: "OFF-1".into(),
            principal_id: "H-1".into(),
            starts_at: Utc.with_ymd_and_hms(2026, 7, 20, 1, 0, 0).unwrap(),
            ends_at: Utc.with_ymd_and_hms(2026, 7, 21, 1, 0, 0).unwrap(),
            reason: "leave".into(),
            created_by: "H-1".into(),
            created_at: friday,
            cancelled_by: None,
            cancelled_at: None,
        });
        let tuesday = add_working_hours(&calendar, friday, 4.0).unwrap();
        assert_eq!(tuesday, Utc.with_ymd_and_hms(2026, 7, 21, 3, 0, 0).unwrap());
    }
}
