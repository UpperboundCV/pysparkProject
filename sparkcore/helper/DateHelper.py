from datetime import datetime, timedelta


class DateHelper:
    DATE_FORMAT: str = '%Y-%m-%d'
    DATE_TIME_FORMAT: str = '%Y-%m-%d %H:%M:%S'

    @classmethod
    def is_today_first_day_of_month(cls, date: str) -> bool:
        return datetime.today().replace(day=1).strftime(cls.DATE_FORMAT) == date

    @classmethod
    def is_today_last_day_of_month(cls, date: str) -> bool:
        return cls.last_day_of_month(date).strftime(cls.DATE_FORMAT) == date

    @classmethod
    def last_day_of_month(cls, date: str) -> str:
        if date.month == 12:
            return date.replace(day=31)
        return date.replace(month=date.month + 1, day=1) - datetime.timedelta(days=1)

    @classmethod
    def today_date(cls) -> str:
        return datetime.today().strftime(cls.DATE_FORMAT)

    @classmethod
    def next_n_date_from_today(cls, n: int) -> str:
        return (datetime.today() + timedelta(days=n)).strftime(cls.DATE_FORMAT)

    @classmethod
    def number_month_diff(cls, end_date: datetime, start_date: datetime) -> int:
        return (end_date.year - start_date.year) * 12 + (end_date.month - start_date.month)

    @classmethod
    def date_str_num_month_diff(cls, end_date: str, start_date: str) -> int:
        end_date_dt = cls.str2datetime(end_date, '-')
        start_date_dt = cls.str2datetime(start_date, '-')
        return cls.number_month_diff(end_date_dt, start_date_dt)

    @classmethod
    def str2datetime(cls, date: str, date_delimiter: str) -> datetime:
        [year, month, date] = date.split(date_delimiter)
        return datetime(int(year), int(month), int(date))

    @classmethod
    def add_days(cls, date: str, date_delimiter: str, n: int) -> str:
        return (cls.str2datetime(date, date_delimiter) + timedelta(days=n)).strftime(cls.DATE_FORMAT)


