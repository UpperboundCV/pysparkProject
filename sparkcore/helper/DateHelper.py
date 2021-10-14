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
