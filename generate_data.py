"""
Generate process mining data

Run from the command-line:
python generate_data.py [input_file] [approx_rows] [--help]
"""

#%%
# Global constants
first_step_id = 'START'
last_step_id = 'END'

# Parse input arguments
import argparse
def parse_input_args():
    parser = argparse.ArgumentParser(description='Generate process mining data using an input file.')
    parser.add_argument(
        'input_file',
        nargs='?',
        default='./input.example.xlsx',
        help='provide the input Excel file (default: input.example.xlsx)'
    )
    parser.add_argument(
        'approx_rows',
        nargs='?',
        type=int,
        default=1000,
        help='provide the approximate number of rows of the output dataset (default: 1000)'
    )
    return parser.parse_args() 

# You can change the unit of duration (seconds, minutes, days, etc) here
from datetime import timedelta
def to_timedelta(x):
    return timedelta(minutes=float(x))

# Inspect a dataframe
import pandas as pd 
def inspect_df(df, n=5):
    print(df.shape)
    try:
        display(df.head(n))
    except NameError:
        print(df.head(n))

# Input file must have sheets: step, flow and optionally case_property and step_property
from xlrd import XLRDError
def read_input(input_file):

    # Load the required sheets in a dict
    d = {
        # Note to self: Always use singular table names. If you ever forget, just google why.
        'step': pd.read_excel(input_file, sheet_name='step', index_col='step_id'),
        'process_flow': pd.read_excel(input_file, sheet_name='process_flow', index_col='step_id')
    }

    # Optional sheets
    try:
        d.update({
            'case_property': pd.read_excel(input_file, sheet_name='case_property', index_col='property_type'),
            'step_property': pd.read_excel(input_file, sheet_name='step_property', index_col='step_id')
        })
    except XLRDError as e:
        print(repr(e))
        pass

    # TODO: assert that
    # Activities: first step and last step must be duration = 0
    # All probability columns: negative probability can not exist
    # All probability columns: all rows must sum to 1, except row N, must all equal 0

    return d

# Process the input dictionary with all Excel sheets
def process_input(d):
    
    # Get the step sheet
    df_step = d['step']

    # Convert the time columns to a timedelta
    df_step.duration = df_step.duration.apply(to_timedelta)
    df_step.wait_time = df_step.wait_time.apply(to_timedelta)

    # Get the process flow sheet and group it into lists per step_id
    df_flow = (
        d['process_flow'].groupby(by='step_id').agg(list)
        .rename(columns={
            'next_step_id': 'next_possible_steps_id',
            'probability': 'next_possible_steps_probability'
        })
    )

    # Update the step dataframe with next possible steps info
    df_step = pd.merge(df_step, df_flow, how='left', on='step_id')

    # TODO: Check the optional sheets
    if 'step_property' in d.keys():
        # Also process the step properties
        # TODO: Something like this returns a useful pivot table
        d['step_property'].pivot_table(
            values=['property', 'probability'], 
            index='step_id', 
            columns='property_type', 
            aggfunc=list
        )
    
    # TODO: Also get, process and return df_case
    return df_step

# Randomize a timedelta
from random import gauss, random
def randomize_duration(duration, might_be_outlier):

    # Could this be an outlier (user input based times 1% chance)?
    if might_be_outlier and random() < 0.01:
        # Outlier between 1 and 10 times the value
        return (1 + 9 * random()) * duration
    else:
        # Else use a normal distribution
        mu = duration
        sigma = mu / 10
        return gauss(mu, sigma)

# The case (a "case" as refered to in process mining) object
from uuid import uuid4
from datetime import datetime
from random import choices
class Case:
    """
    A case object which can walk through the process and always *is* at some 
    step at some point in time.
    """

    # Initiate attributes of a new instance of Case
    def __init__(self):
        self.uuid = str(uuid4())
        self.now = datetime.now() #TODO: Randomize this
        self.done = False

        # Initiate current state with some values in a dictionary
        # Note: Don't try slicing the last row of self.history. You won't be able to set values properly. Read http://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#why-does-assignment-fail-when-using-chained-indexing
        self.current = {
            'case_id': self.uuid,
            'step_id': first_step_id,
            'start_time': self.now,
            'end_time': None
        }

        # Also keep track of all previous steps in a dataframe
        self.history = pd.DataFrame()

    def end_current_step(self):
        
        # Proceed to the end of the step
        self.now += randomize_duration(
            df_step.loc[self.current['step_id'], 'duration'],
            df_step.loc[self.current['step_id'], 'outliers']
        )
        
        # This moment is the end time of the current step
        self.current['end_time'] = self.now

        # The step is finished so write the current status to history
        self.history = self.history.append(self.current, ignore_index=True)

    def wait_after_step(self):

        # Add some wait time, that's all for now
        self.now += randomize_duration(
            df_step.loc[self.current['step_id'], 'wait_time'],
            False
        )

    def go_to_next_step(self):

        # First end the current step
        self.end_current_step()

        # When we've just ended the final step we can stop here
        if self.current['step_id'] == last_step_id:
            self.done = True
            return

        # Then wait
        self.wait_after_step()

        # And choose (random) the next step
        # TODO: Maybe, instead of using full df_step, pass df_step.loc[this_stepid, :] to this function
        next_step_id = choices(
            df_step.loc[self.current['step_id'], 'next_possible_steps_id'], 
            df_step.loc[self.current['step_id'], 'next_possible_steps_probability']
        )[0]

        # Actually start the next step by creating a new current status
        self.current = {
            'case_id': self.uuid,
            'step_id': next_step_id,
            'start_time': self.now,
            'end_time': None
        }

    def walk_through_process(self):

        # Keep proceeding to the next step until the case is done. Makes sense right?
        while not self.done:
            self.go_to_next_step()
        


"""
Process layout

  x---------------x---------------x                             x-----------------x
START                           1                               END
init
    end_current_step     wait_after_step
            go_to_next_step


"""

# Some specific tasks to add info to the basic dataset
def post_processing(df):

    # Add step name to each step id
    df = df.merge(
        df_step.loc[:, 'step_name'],
        how='left',
        left_on='step_id',
        right_index=True
    )

    # TODO: Add step properties here

    # TODO: Add case properties here

    # Reset the index, just for neatness
    df.reset_index(drop=True, inplace=True)

    return df

#%% 
"""
Main
"""
import time
if __name__ == "__main__":
    
    # Start timer
    timer = time.time()

    # Parse command-line input arguments
    args = parse_input_args()

    # Read and process the sheets from the input Excel
    print('Reading input from:', args.input_file)
    df_step = process_input(read_input(args.input_file))
    
    # Note: If you are new to this script, definitely take a look at this dataframe
    #inspect_df(df_step, n=10)

    # Generate random cases until the dataframe is full
    df = pd.DataFrame()
    print('Processing row:')
    while len(df) < args.approx_rows:
        
        # Initialize a random case
        case = Case()

        # Walk through the entire process
        case.walk_through_process()

        # Extract the history and append to the main dataframe
        df = df.append(case.history)
        print('\r' + str(len(df)), end='')

    # Now the basics dataset is done, but let's add some extra specific info
    df = post_processing(df)

    # Stop the timer
    print('\nDone in: %.1f sec' % (time.time() - timer))

    # Inspect
    print('Shape of output table: ', end='')
    inspect_df(df, n=10)

    # Save to file
    from os import makedirs
    makedirs('./output', exist_ok=True)
    df.to_csv('./output/process_data.csv', index=False)
    print('Dataset saved in: output/process_data.csv')
