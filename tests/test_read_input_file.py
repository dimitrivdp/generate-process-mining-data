"""
Tests
"""
import pandas as pd 

# Read input file, returns a dictionary
from generate_data import read_input_file
d = read_input_file('input.example.xlsx')
print(d.keys())


# Process the returned input dictionary
from generate_data import merge_flow_into_steps
df_activity = merge_flow_into_steps(d)



#
print('End')
