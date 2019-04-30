"""
Tests
"""

# Read input file
from generate_data import read_input
filename = 'input.sample.xlsx'
df = read_input(filename)
print(df.keys())


print('End')
