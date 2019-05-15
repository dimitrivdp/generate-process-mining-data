"""
Tests
"""
import pandas as pd 

# Read input file
from generate_data import read_input
d = read_input('input.example.xlsx')
print(d.keys())


# Now combine the sheets with index = activity_id so we can easily pass it around
from generate_data import process_input
df_activity = process_input(d)



#
print('End')
