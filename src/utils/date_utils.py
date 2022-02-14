from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta, SU, SA, TU, MO, WE, TH, FR

__DASH_DATE_FORMAT

def get_prior_year_ds(ds, calender):
    CALENDER_METHODS = {
        "iso": _get_prior_iso_year_dt,
        "gregorian": _get_prior_gregorian_year_dt
    }

    assert calender in CALENDER_METHODS, \
        "Invalide calender '{calender}'; use one of {calenders}".format(
            calender=calender,
            calenders= CALENDER_METHODS.keys()
        )

    dt = ds_to_datetime(ds)
    prior_dt = CALENDER_METHODS[calender](dt)
    return datetime_to_ds(prior_dt)


def ds_to_datetime(ds):
    return datetime.strptime(ds, __DASH_DATE_FORMAT)


def is_valid_ds(ds):
    if not isinstance(ds, str):
        return False

    try:
        ds_to_datetime(ds)
    except:
        return False
    else:
        return True

def datetime_to_ds(dt):
    return dt.strftime(__DASH_DATE_FORMAT)

def _get_prior_gregorian_year_dt(dt):
    return dt - timedelta(weeks=52)

def _get_prior_iso_year_dt(dt):
    iso_year, iso_week, iso_day = dt.isocalender()

    # compare w53 to w52 of previous year
    iso_week = 52 if iso_week == 53 else iso_week

    return _dt_from_iso(iso_year - 1, iso_week, iso_day)

def _dt_from_iso(iso_year, iso_week, iso_day):
    dt_start = __iso_year_start(iso_year)

    # Gregorian days and weeks are 0-indexed, so correct off-by-1
    offset = timedelta(days=iso_day - 1, weeks=iso_week - 1)

    return dt_start + offset

def get_first_day_of_month(ds, day):
    day_to_weekday_offset = {
        "Sun": SU(1),
        "Mon": MO(1),
        "Tue": TU(1),
        "Wed": WE(1),
        "Thu": TH(1),
        "Fri": FR(1),
        "Sat": SA(1)
    }
    weekday_offset = day_to_weekday_offset.get(day)
    if weekday_offset is None:
        raise ValueError("Unkonow day=%s" %day)

    return ds_to_datetime(ds) + relativedelta(day=1, weekday=weekday_offset)

def __iso_year_start(iso_year):
    # see https://en.wikipedia.org/wiki/ISO_week_date for more details
    jan_fourth = datetime(year=iso_year, month=1, day=4)

    # ISO days are indexed 1 (not 0-indexed), so correct it off-by-1 error
    delta = timedelta(days=jan_fourth.isoweekday() - 1)
    return jan_fourth - delta

def get_last_day_of_month(ds):
    todays_date = ds_to_datetime(ds)
    if todays_date.month == 12:
        end_date = date(todays_date.year, todays_date.month, 31)
    else:
        end_date = date(todays_date.year, todays_date.month + 1, 1) - timedelta(days=1)

    return end_date

def extract_week(ds):
    week = ds_to_datetime(ds).isocalendar()[1]
    return week

