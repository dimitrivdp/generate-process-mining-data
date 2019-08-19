"""
Tests
"""
#%%
import pandas as pd 
from datetime import timedelta
import matplotlib.pyplot as plt

from generate_data import randomize_timedelta

# Set a static timedelta of 10 mins
dt = timedelta(minutes = 10)

# Call randomize_duration
outlier = True
randomized_dt = [randomize_timedelta(dt, outlier) for i in range(100)]

# Convert to seconds to visualize
randomized_dt = [dt.total_seconds() for dt in randomized_dt]

# Visualize
plt.hist(randomized_dt, bins=100)
plt.show()

