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
        default=100,
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

# Input file must have sheets: step, flow and optionally case_properties and step_properties
from xlrd import XLRDError
def read_input_file(input_file):

    # Load the required sheets in a dict
    d = {
        'steps': pd.read_excel(input_file, sheet_name='steps', index_col='step_id'),
        'process_flow': pd.read_excel(input_file, sheet_name='process_flow', index_col='step_id')
    }

    # Optional sheets
    try:
        d.update({'case_properties': pd.read_excel(input_file, sheet_name='case_properties', index_col='property_type')})
    except XLRDError as e:
        print(repr(e))
        pass
    try:
        d.update({'step_properties': pd.read_excel(input_file, sheet_name='step_properties', index_col='step_id')})
    except XLRDError as e:
        print(repr(e))
        pass

    # TODO: assert that
    # Activities: first step and last step must be duration = 0
    # All probability columns: negative probability can not exist
    # All probability columns: all rows must sum to 1, except row N, must all equal 0

    return d

# Process the input dictionary with all Excel sheets
def process_input_file(d):
    
    # Get the step sheet as a dataframe
    df_step = d['steps']

    # Convert the time columns to a timedelta
    df_step.duration = df_step.duration.apply(to_timedelta)
    df_step.wait_time = df_step.wait_time.apply(to_timedelta)

    # Get the process flow sheet and group it into lists per step_id
    df_flow = (
        d['process_flow']
        .groupby(by='step_id').agg(list)
        .rename(columns={
            'next_step_id': 'next_possible_steps_id',
            'probability': 'next_possible_steps_probability'
        })
    )

    # Join the step dataframe with next possible steps info
    df_step = pd.merge(df_step, df_flow, how='left', on='step_id')

    # Optionally process the optional sheets
    if 'case_properties' in d.keys():
        df_case_properties = d['case_properties'].groupby(by='property_type').agg(list)

    # TODO: Check the optional sheets
    if 'step_properties' in d.keys():
        # Also process the step properties
        # TODO: Something like this returns a useful pivot table
        d['step_properties'].pivot_table(
            values=['property', 'probability'], 
            index='step_id', 
            columns='property_type', 
            aggfunc=list
        )
    
    return (df_step, df_case_properties)

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
        self.uuid = str(uuid4())[:8]
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
            df_steps.loc[self.current['step_id'], 'duration'],
            df_steps.loc[self.current['step_id'], 'has_outliers']
        )
        
        # Now is the end time of the current step
        self.current['end_time'] = self.now

        # This step is done so write the current status to history
        self.history = self.history.append(self.current, ignore_index=True)

    def wait_after_step(self):

        # Add some wait time, that's all for now
        self.now += randomize_duration(
            df_steps.loc[self.current['step_id'], 'wait_time'],
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
        next_step_id = choices(
            df_steps.loc[self.current['step_id'], 'next_possible_steps_id'], 
            df_steps.loc[self.current['step_id'], 'next_possible_steps_probability']
        )[0]

        # Actually start the next step by creating a new current status
        self.current = {
            'case_id': self.uuid,
            'step_id': next_step_id,
            'start_time': self.now,
            'end_time': None
        }

    def walk_through_process(self):
        """
        Process layout and corresponding methods

        START       __init__()
          |         end_current_step()
          x                                   > go_to_next_step()
          |         wait_after_step()
         (1)
          |         end_current_step()
          x                                   > go_to_next_step()
          |         wait_after_step()
         (2) 
          .
          .
          .
         END
          |         end_current_step()
          x         self.done = True
        """

        # Keep proceeding to the next step until the case is done. Makes sense right?
        while not self.done:
            self.go_to_next_step()

# Some specific tasks to add info to the basic dataset
def post_processing(df):

    """ Post processing regarding individual steps """
    # Add the step name to each corresponding step id
    df = df.merge(
        df_steps.loc[:, 'step_name'],
        how='left',
        left_on='step_id',
        right_index=True
    )

    # TODO: Add step properties here


    """ Post processing regarding cases """
    # Give each case_id the properties supplied in the input file
    def determine_the_case_properties(unique_case_ids):
            
        # Get all unique case_ids as a dataframe
        unique_case_ids = pd.DataFrame({'case_id': unique_case_ids})

        # For each case property in the input file, choose a value and put it in an extra column
        for property_type in df_case_properties.index:
            unique_case_ids[property_type] = choices(
                list(df_case_properties.loc[property_type, 'value']),
                list(df_case_properties.loc[property_type, 'probability']),
                k=len(unique_case_ids)
            )
        
        return unique_case_ids

    # Get properties for every case_id and join them to our main dataframe
    unique_cases_with_properties = determine_the_case_properties(df.case_id.unique())
    df = df.merge(unique_cases_with_properties, how='left', on='case_id')


    """ General post processing """
    # TODO: Assert no duplicate UUIDs

    # Reset the index, just for neatness
    df.reset_index(drop=True, inplace=True)

    return df



#%% 
"""
Main
"""
import time
if __name__ == "__main__":
    
    # Start timer - only to record script run time
    timer = time.time()

    # Parse command-line input arguments
    args = parse_input_args()

    # Read and process the sheets from the input Excel
    print('Reading input from:', args.input_file)
    (df_steps, df_case_properties) = process_input_file(read_input_file(args.input_file))
    
    # Note: If you are new to this script, definitely take a look at this dataframe
    #inspect_df(df_steps, n=10)

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

    # The basic dataset is done, but let's add some extra info
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
