#
from generate_data import parse_argv

(i, r) = parse_argv("default.xlsx", 123)
assert i == "default.xlsx"
assert r == 123

#
from generate_data import to_timedelta

dt = to_timedelta(10, td_unit="hours") 
assert dt.total_seconds() == 10*60*60

