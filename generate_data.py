"""Generate a random process mining dataset.

Run from the command line:
python generate_data.py [input_filename] [approx_rows]
"""

import sys
from os import makedirs
from datetime import timedelta, datetime
import time
import pandas as pd
from random import gauss, random, choices
from uuid import uuid4


# Global constants
START = "START"
END = "END"
DURATION_UNIT = "minutes"


def parse_argv(input_filename="examples/process_with_loops.xlsx", approx_rows=100):
    # argv = argument vector
    n = len(sys.argv)
    if n > 1:
        input_filename = sys.argv[1]
    if n > 2:
        approx_rows = int(sys.argv[2])
    return (input_filename, approx_rows)


def to_timedelta(x, td_unit="minutes"):
    return eval("timedelta(" + td_unit + "=float(x))")


def inspect_df(df, n=5):
    print(df.shape)
    try:
        display(df.head(n))
    except NameError:
        print(df.head(n))


def read_input_file(input_filename):
    df_steps = pd.read_excel(input_filename, sheet_name="steps", index_col="step_id")

    # TODO: Add optional columns: wait_time, duration_outliers, wait_time_outliers

    # Convert the time columns to a timedelta
    df_steps.duration = df_steps.duration.apply(to_timedelta)
    df_steps.wait_time = df_steps.wait_time.apply(to_timedelta)

    df_flow = pd.read_excel(
        input_filename, sheet_name="process_flow", index_col="step_id"
    )

    # TODO: assert that
    # Activities: first step and last step must be duration = 0
    # Datatypes: timedeltas and bools
    # All probability columns: negative probability can not exist
    # All probability columns: all rows must sum to 1, except row N, must all equal 0
    return (df_steps, df_flow)


def merge_flow_into_steps(df_steps, df_flow):
    # Group the flow dataframe into lists for easy usage later on with random.choices()
    df_flow_grouped = (
        df_flow.groupby(by="step_id")
        .agg(list)
        .rename(
            columns={
                "next_step_id": "next_possible_steps_id",
                "probability": "next_possible_steps_probability",
            }
        )
    )
    # Join the step dataframe with next possible steps info
    df_steps = pd.merge(df_steps, df_flow_grouped, how="left", on="step_id")
    return df_steps


def randomize_duration(duration, has_outliers=False):
    # If the distribution has outliers, 1% will be an outlier
    if has_outliers and random() < 0.01:
        # Outlier lays between 1 and 10 times the original value
        return (1 + 9 * random()) * duration
    else:
        # If not an outlier, use a normal distribution
        mu = duration
        sigma = mu / 10
        return gauss(mu, sigma)


class Case:
    """A case object with a unique id.
    
    Each case starts at `start_step_id` and walks through the process as defined in the input Excel file.
    It's current state (step and time) is defined in self.current and self.history is a list of all completed steps.
    
    """

    # Initiate attributes of a new instance of Case
    def __init__(self):
        self.uuid = str(uuid4())
        self.clock = datetime.now()  # TODO: Randomize this
        self.done = False

        # Initiate current state with some values in a dictionary
        # Note: Don't try slicing the last row of self.history. You won't be able to set values properly. Read http://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#why-does-assignment-fail-when-using-chained-indexing
        self.current = {
            "case_id": self.uuid,
            "step_id": START,
            "start_time": self.clock,
            "end_time": None,
        }

        # Also keep track of all previous steps in a dataframe
        self.history = pd.DataFrame()

    def end_current_step(self):
        # Proceed to the end of the step
        self.clock += randomize_duration(
            df_steps.loc[self.current["step_id"], "duration"],
            df_steps.loc[self.current["step_id"], "duration_outliers"],
        )

        # Now is the end time of the current step
        self.current["end_time"] = self.clock

        # This step is completed so write the current status to history
        self.history = self.history.append(self.current, ignore_index=True)

    def wait_after_step(self):
        # Add some wait time, that's all for now
        self.clock += randomize_duration(
            df_steps.loc[self.current["step_id"], "wait_time"],
            df_steps.loc[self.current["step_id"], "wait_time_outliers"],
        )

    def go_to_next_step(self):
        # First end the current step
        self.end_current_step()

        # If we've just ended the final step we can stop here
        if self.current["step_id"] == END:
            self.done = True
            return

        # Then wait
        self.wait_after_step()

        # And choose (random) the next step
        next_step_id = choices(
            df_steps.loc[self.current["step_id"], "next_possible_steps_id"],
            df_steps.loc[self.current["step_id"], "next_possible_steps_probability"],
        )[0]

        # Actually start the next step by creating a new current status
        self.current = {
            "case_id": self.uuid,
            "step_id": next_step_id,
            "start_time": self.clock,
            "end_time": None,
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

        # Keep proceeding to the next step until the case is done. Simple right?
        while not self.done:
            self.go_to_next_step()


def post_processing(df):
    # Join step names
    df = df.merge(
        df_steps.loc[:, "step_name"], how="left", left_on="step_id", right_index=True
    )

    # Remove START and END
    df = df[(df.step_id != START) & (df.step_id != END)]

    print("Shape of output table: ", end="")
    inspect_df(df, n=10)
    return df


if __name__ == "__main__":
    # Start a timer - only to record script run time
    timer = time.time()

    # Parse command line arguments (input filename and approximate number of rows)
    (input_filename, approx_rows) = parse_argv()

    # Read and process the sheets from the input Excel
    print("Reading input from:", input_filename)
    (df_steps, df_flow) = read_input_file(input_filename)
    df_steps = merge_flow_into_steps(df_steps, df_flow)

    # Note: If you are new to this script, definitely take a look at this dataframe
    inspect_df(df_steps, n=10)

    # Generate random cases until the dataframe is full
    df = pd.DataFrame()
    print("Processing row:")
    while len(df) < approx_rows:
        # Start a case and walk through the process
        case = Case()
        case.walk_through_process()

        # Extract the history and append to the main dataframe
        df = df.append(case.history)
        print("\r" + str(len(df)), end="")

    # The basic dataset is done, but needs some specific tasks
    df = post_processing(df)

    print("\nDone in: %.1f sec" % (time.time() - timer))


    # Save to file
    makedirs("./output", exist_ok=True)
    # TODO: use inputfile name .csv
    #df.to_csv("./output/process_data.csv", index=False)
    print("Dataset saved in: output/process_data.csv")
