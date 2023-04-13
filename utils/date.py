import datetime as dt

today = dt.datetime.today()
year = f'{today.year}'
month = f'{today.month:02d}'
day = f'{today.day:02d}'

def increment_day(year: str, month: str, day: str, n_days: int) -> tuple[str, str, str]:
    """
    Increment the input date by one day.
    :param year:
    :param month:
    :param day:
    :return:
    """

    # Create a datetime object from the input da    te
    date = dt.datetime(int(year), int(month), int(day))

    # Add one day to the date
    updated_date = date + dt.timedelta(days=n_days)

    # Extract the year, month, and day from the updated date
    updated_year = updated_date.year
    updated_month = updated_date.month
    updated_day = updated_date.day

    return f'{updated_year:04d}', f'{updated_month:02d}', f'{updated_day:02d}'