"""
Tests
"""
import pandas as pd 

# Read input file, returns a dictionary
from generate_data import read_input_file
d = read_input_file('input.example.xlsx')
print(d.keys())


# Process the returned input dictionary
from generate_data import process_input_file
df_activity = process_input_file(d)



#
print('End')
