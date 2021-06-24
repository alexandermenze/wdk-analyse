from datetime import time
import numpy as np
import operator


class WeekdayStatistic(object):
    def __init__(self, weekday=None, count=None, days_positive=None, days_negative=None, total_positive=None, total_negative=None):
        self.weekday = weekday
        self.count = count
        self.days_positive = days_positive
        self.days_negative = days_negative
        self.total_positive = total_positive
        self.total_negative = total_negative
        self.days_ratio = (days_positive / np.float64(days_negative))
        self.total_ratio = (total_positive / np.float64(-total_negative))


class TimeRange(object):
    def __init__(self, name=None, start=None, end=None):
        self.name = name
        self.start = start
        self.end = end


class IndexTimeRanges(object):
    def __init__(self, index_name, isin, time_ranges):
        self.index_name = index_name
        self.isin = isin
        self.time_ranges = time_ranges


def create_weekday_statistic(data, close_last_to_open=True):
    return list([
        WeekdayStatistic(i, __count_days_by_weekday(data, i), __count_days_pos_neg(data, i, True, close_last_to_open), __count_days_pos_neg(
            data, i, False, close_last_to_open), __sum_value_changes(data, i, True, close_last_to_open), __sum_value_changes(data, i, False, close_last_to_open))
        for i
        in range(0, 5)
    ])


def __count_days_by_weekday(data, weekday):
    # Count number of specified weekdays in dataset
    return len([s for s in data if s.date.weekday() == weekday])


def __count_days_pos_neg(data, weekday, count_positive, close_last_to_open):
    # Count positive or negative days in dataset

    last_value = 0
    days = 0

    for d in sorted(data, key=operator.attrgetter('date')):
        if d.date.weekday() == weekday:
            curr_value = d.close - \
                last_value if last_value != 0 and close_last_to_open else d.close - d.open

            if count_positive and curr_value >= 0:
                days += 1
            elif not count_positive and curr_value < 0:
                days += 1

        last_value = d.close

    return days


def __sum_value_changes(data, weekday, positive_change, close_last_to_open):
    # Sum the total positive or negative value changes

    last_close = 0
    value = 0

    for d in sorted(data, key=operator.attrgetter('date')):
        if d.date.weekday() == weekday:
            curr_value = d.close - \
                last_close if last_close != 0 and close_last_to_open else d.close - d.open

            if positive_change and curr_value >= 0:
                value += curr_value
            elif not positive_change and curr_value < 0:
                value += curr_value

        last_close = d.close

    return value
