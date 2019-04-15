"""
Generate process mining data

"""

#%%
# Input arguments
# TODO: also parse these as input argument
approx_max_rows = 1000
input_file = './input.sample.xlsx'

# Change here the unit (seconds, minutes, days, etc)
from datetime import timedelta
def to_timedelta(x):
    return timedelta(minutes=float(x))

# Fcn: determine the next activity based on the current activity and the probability table from input file
from random import choices
def determine_next_activity(current_activity):

    # Get current probabilities from probability table
    next_possible_activities = df_flow.loc[current_activity, 'next_activity_id']
    probabilities = df_flow.loc[current_activity, 'probability']
    
    # Determine the next activity
    if type(probabilities) is not pd.core.series.Series and probabilities == 1:
        # Just 1 option
        return next_possible_activities
    else:
        # Choose
        next_activity = choices(list(next_possible_activities), list(probabilities))[0]
        return next_activity

# Generate a random activity and a full process run
from uuid import uuid4
def generate_random_activity():
    
    # Initialize a random activity which starts at the first activity at t0
    case_id = str(uuid4())[:8]
    df = pd.DataFrame.from_dict({
        'case_id': [case_id],
        'activity_id': [first_activity],
        'start_time': [t0]
    })
    
    # Run the activity through the process
    while True:

        # Get current activity as the last entry in the dataframe
        last_row = df.tail(1)
        current_activity = last_row.loc[0, 'activity_id']
        
        # Determine next activity and its start time
        next_activity = determine_next_activity(current_activity)
        next_activity_start_time = last_row.loc[0, 'start_time'] + to_timedelta(df_activities.loc[current_activity, 'duration'])
        # TODO: incorporate wait time in the input.xlsx and end time in the output

        # Append to dataframe
        df = df.append(pd.DataFrame.from_dict({
            'case_id': [case_id],
            'activity_id': [next_activity],
            'start_time': [next_activity_start_time]
        }))

        # Stop if arrived at last activity
        if next_activity == last_activity:
            break
    
    return df

# Input file must have sheets: activitys, flow and optionally activity_properties and activity_properties
def read_input():
    # TODO: parse and return act_prop and activity_prop sheets if exist

    # Read basic info for each process activity
    df_activities = pd.read_excel(input_file, sheet_name='activities', index_col='activity_id')
    # TODO: first activity and last activity must be duration = dur_stdev = 0

    # Read the process flow
    df_flow = pd.read_excel(input_file, sheet_name='process_flow', index_col='activity_id')
    # TODO: have some requirements:
    # negative number can not exist
    # all rows must sum to 1, except row n must all equal 0

    return (df_activities, df_flow)

#%% 
"""
Main
"""
# Read input
import pandas as pd
(df_activities, df_flow) = read_input()

# Set (hard coded) first and last activity of the process
first_activity = 0
last_activity = 'N'
print('First activity:', first_activity)
print('Last activity:', last_activity)

# Set t0
# TODO: For now, all activities start at t0. This should become some time span
from datetime import datetime
t0 = datetime.now()

# Initialize dataframe
df = pd.DataFrame()

# Generate data
while len(df) < approx_max_rows:
    df = df.append(generate_random_activity())

# Attach activity name
df = df.merge(df_activities.loc[:, 'activity_name'], how='left', left_on='activity_id', right_index=True)

# Inspect
try:
    # Only Jupyter will display
    print(df.shape)
    display(df.describe())
    display(df.head())
except NameError:
    pass

#%%
# Save to file
from os import makedirs
makedirs('./output', exist_ok=True)
df.to_csv('./output/process_data.csv', index=False)
