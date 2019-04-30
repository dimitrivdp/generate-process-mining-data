"""
Generate process mining data
"""

#%%
# Global variables
# TODO: Also parse these as input argument
approx_max_rows = 1000
input_file = './input.sample.xlsx'

# Change here the duration unit (seconds, minutes, days, etc)
from datetime import timedelta
def to_timedelta(x):
    return timedelta(minutes=float(x))

# Inspect dataframe
def inspect_df(df):
    print(df.shape)
    try:
        display(df.head())
    except NameError:
        print(df.head())

# Input file must have sheets: activity, flow and optionally case_property and activity_property
from xlrd import XLRDError
def read_input(input_file):

    # Required sheets
    df = {
        # Note to self: Always use singular table names. If you ever forget, just google why.
        'activity': pd.read_excel(input_file, sheet_name='activity', index_col='activity_id'),
        'flow': pd.read_excel(input_file, sheet_name='process_flow', index_col='activity_id')
    }

    # Optional sheets
    try:
        df.update({
            'case_property': pd.read_excel(input_file, sheet_name='case_property', index_col='property_type'),
            'activity_property': pd.read_excel(input_file, sheet_name='activity_property', index_col='activity_id')
        })
    except XLRDError as e:
        print(repr(e))
        pass

    # TODO: assert that
    # Activities: first activity and last activity must be duration = 0
    # All probability columns: negative probability can not exist
    # All probability columns: all rows must sum to 1, except row N, must all equal 0

    return df

# Generate a random case which runs through the entire process
from uuid import uuid4
def generate_random_case(df_input):
    
    # Initialize a random activity which starts at the first activity at t0
    case_id = str(uuid4())
    df = pd.DataFrame.from_dict({
        'case_id': [case_id],
        'activity_id': [first_activity],
        'start_time': [t0]
        # TODO: Variate t0 for different cases
    })
    
    # Run the activity through the process activities
    while True:

        # Get current activity, which is the last row in the dataframe
        current_activity = df.iloc[-1]
                
        # Determine next activity and its start time
        next_activity = determine_next_activity(current_activity)
        next_activity_start_time = determine_next_activity_start_time(current_activity)
        # TODO: wait time in the input.xlsx and end time in the output

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

# Determine the next activity based on the current activity and the probability table from input file
from random import choices
def determine_next_activity(current_activity):

    # Get current probabilities from input file
    # TODO: Prepare these lists & probs in one table. Huge performance win here
    current_activity_id = current_activity.activity_id
    next_possible_activities = df_input['flow'].loc[current_activity_id, 'next_activity_id']
    probabilities = df_input['flow'].loc[current_activity_id, 'probability']
    
    # Determine the next activity
    if type(probabilities) is not pd.core.series.Series and probabilities == 1:
        # Just 1 option
        return next_possible_activities
    else:
        # Choose
        next_activity = choices(list(next_possible_activities), list(probabilities))[0]
        return next_activity

# Determine start time of the next activity based on the current activity's duration
from random import gauss, random
def determine_next_activity_start_time(current_activity):

    # Get info from input file
    # TODO: Prepare this info in one activity table and use this in current_activity. For performance
    current_activity_id = current_activity.activity_id
    current_activity_info = df_input['activity'].loc[current_activity_id, :]

    # Could this be an outlier (user input based and 1% chance)?
    if current_activity_info.outliers and random() < 0.01:
        # Outlier between 1 and 10 times the duration
        duration = (1 + 9 * random()) * current_activity_info.duration
    else:
        # Else use a normal distribution
        mu = current_activity_info.duration
        sigma = mu / 10
        duration = gauss(mu, sigma)

    # Add to the current activity's start time
    return current_activity.start_time + to_timedelta(duration)

#%% 
"""
Main
"""
import pandas as pd
if __name__ == "__main__":

    # Read input
    df_input = read_input(input_file)

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

    # Generate random cases based on the input file
    while len(df) < approx_max_rows:
        df = df.append(generate_random_case(df_input))

    # Add activity name column
    df = df.merge(df_input['activity'].loc[:, 'activity_name'], how='left', left_on='activity_id', right_index=True)

    # Inspect
    inspect_df(df)

    # Save to file
    from os import makedirs
    makedirs('./output', exist_ok=True)
    df.to_csv('./output/process_data.csv', index=False)


