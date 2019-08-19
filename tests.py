#
from generate_data import parse_argv

(i, r) = parse_argv("default.xlsx", 123)
assert i == "default.xlsx"
assert r == 123

#
from generate_data import to_timedelta

dt = to_timedelta(10, td_unit="hours") 
assert dt.total_seconds() == 10*60*60

#
from generate_data import random_datetime_between
from datetime import datetime, date

dt1 = datetime(2019, 1, 1)
dt2 = datetime(2019, 1, 1)
dts = [dt1, dt2]
dt3 = random_datetime_between(*dts)
assert dt3.date() == date(2019, 1, 1)

