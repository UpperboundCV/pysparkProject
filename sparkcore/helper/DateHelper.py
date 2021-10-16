from datetime import datetime


class DateHelper:
    DATE_FORMAT: str = '%Y-%m-%d'
    DATE_TIME_FORMAT: str = '%Y-%m-%d %H:%M:%S'

    def is_today_first_day_of_month(cls, date: str) -> bool:
        return datetime.today().replace(day=1).strftime(cls.DATE_FORMAT) == date

    def is_today_last_day_of_month(cls, date: str) -> bool:
        return cls.last_day_of_month(date).strftime(cls.DATE_FORMAT) == date

    def last_day_of_month(cls, date: str) -> str:
        if date.month == 12:
            return date.replace(day=31)
        return date.replace(month=date.month + 1, day=1) - datetime.timedelta(days=1)

    def today_date(cls) -> str:
        return datetime.today().strftime(cls.DATE_FORMAT)

    def number_month_diff(cls, end_date: datetime, start_date: datetime) -> int:
        return (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)

    def date_str_num_month_diff(cls, end_date: str, start_date: str, date_delimiter: str) -> int:
        end_date_dt = cls.str2datetime(end_date, date_delimiter)
        start_date_dt = cls.str2datetime(start_date, date_delimiter)
        return cls.number_month_diff(end_date_dt, start_date_dt)

    def str2datetime(cls, date: str, date_delimiter: str) -> datetime:
        [year, month, date] = date.split(date_delimiter)
        return datetime.datetime(year, month, date)
