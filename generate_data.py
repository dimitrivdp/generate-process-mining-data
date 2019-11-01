"""Generate a random process mining dataset.

Run from the command line (without brackets):
python generate_data.py [input_path] [approx_rows]

Or run in your IDE without any arguments. Just change the global constants below.
"""

import sys
from os import makedirs
from os.path import join, split, splitext
from datetime import timedelta, datetime, time
import time as times
import pandas as pd
from random import gauss, random, choices
from uuid import uuid4
from pprint import pprint


# Global constants
INPUT_PATH = "data/example/Example_process.xlsx"
APPROX_ROWS = 1000
DURATION_UNIT = "minutes"
START = "<Start>"
END = "<End>"
DATASET_TIME_RANGE = [datetime(2016, 1, 1), datetime(2018, 12, 31)]
WORKING_HOURS = [time(hour=9), time(hour=17)]
WORKING_WEEKEND = False


def parse_argv(input_path=None, approx_rows=None):
    # argv = argument vector
    n = len(sys.argv)
    input_path = sys.argv[1] if n > 1 else input_path
    approx_rows = int(sys.argv[2]) if n > 2 else approx_rows
    return (input_path, approx_rows)


def inspect_df(df, n=5):
    print(df.shape)
    print(df.head(n))


def to_timedelta(x, td_unit=DURATION_UNIT):
    return eval("timedelta(" + td_unit + "=float(x))")


def read_input_file(input_path):
    # Required columns: step_id, duration
    # Optional columns: step_name, wait_time, duration_outliers, wait_time_outliers and other custom columns
    excel_activities = pd.read_excel(
        input_path, sheet_name="Activities", index_col="ActivityId"
    )
    # TODO: assert column names
    # Fill NaN
    excel_activities.DurationActivity.fillna(value=0, inplace=True)
    excel_activities.DurationActivity = excel_activities.DurationActivity.apply(
        to_timedelta
    )

    # Flow sheet has a fixed layout: step_id, next_step_id and probability
    excel_flow = pd.read_excel(input_path, sheet_name="Flow", index_col="ActivityId")
    excel_flow.DurationIdle.fillna(value=0, inplace=True)
    excel_flow.DurationIdle = excel_flow.DurationIdle.apply(to_timedelta)

    # TODO: assert that
    # Activities: first step and last step must be duration = 0
    # Datatypes: timedeltas and bools, etc
    # All probability columns: negative probability can not exist
    # All probability columns: all rows must sum to 1, except row N, must all equal 0
    return (excel_activities, excel_flow)


def merge_flow_into_steps(excel_activities, excel_flow):
    # Group the flow into lists per step_id to use with random.choices()
    excel_flow_grouped = (
        excel_flow.groupby(by="ActivityId")
        .agg(list)
        .rename(
            columns={
                "ActivityIdTarget": "ActivityIdPossibleTargets",
                "Probability": "ProbabilityPossibleTargets",
            }
        )
    )
    # Join the steps dataframe with grouped flow
    process_description = pd.merge(
        excel_activities, excel_flow_grouped, how="left", on="ActivityId"
    )
    return process_description


def randomize_timedelta(td, has_outliers=False):
    # If the distribution has outliers, 1% will be an outlier
    if has_outliers and random() < 0.01:
        # Outlier lays between 1 and 10 times the original value
        return (1 + 9 * random()) * td
    else:
        # If not an outlier, use a normal distribution
        mu = td
        sigma = mu / 10
        return gauss(mu, sigma)


def random_datetime_between(dt1, dt2):
    # Returns a datetime dt1 <= N < dt2
    td = dt2 - dt1
    random_td = timedelta(seconds=random() * td.total_seconds())
    return dt1 + random_td


def is_working_time(dt):
    if not WORKING_WEEKEND and dt.weekday() > 4:
        return False
    elif dt.time() < WORKING_HOURS[0] or dt.time() > WORKING_HOURS[1]:
        return False
    else:
        return True


class Case:
    """A case object with an unique id. A case object walks through the process from
    START to END following the process description in the input Excel file. 
    
        self.current: The current state of the case (step and time)
        self.history: A list of all completed steps
    
    """

    # Initiate attributes of a new instance of Case
    def __init__(self, process_description):
        self.uuid = str(uuid4())
        self.clock = datetime(2019, 4, 1)  
        # random_datetime_between(*DATASET_TIME_RANGE)
        self.process_description = process_description


        # Initiate current state with some values in a dictionary
        self.current = {
            "CaseId": self.uuid,
            "ActivityId": START,
            "Timestamp": self.clock,
            "TimestampEnd": None,
            # And insert the values of the process description for this step
            **self.process_description.loc[START].to_dict(),
        }

        # Keep track of all completed steps in a list
        self.history = []

    def do_current_activity(self):
        # Execute the step, so time will pass
        self.clock += randomize_timedelta(self.current["DurationActivity"])

        # Register the end time of the step
        self.current["TimestampEnd"] = self.clock

        # This step is completed so write the current status to history
        self.history.append(self.current)

    def wait_after_step(self):
        # Add some wait time, that's all for now
        self.clock += randomize_timedelta(
            self.current["wait_time"], self.current["wait_time_outliers"]
        )

    def choose_target_activity(self):
        # Choose (random) the next step
        target = choices(
            range(len(self.current["ProbabilityPossibleTargets"])),
            self.current["ProbabilityPossibleTargets"],
        )[0]

        # Actually start the next step by creating a new current status
        self.current = {
            "CaseId": self.uuid,
            "ActivityId": self.current["ActitityIdPossibleTargets"][target],
            "Timestamp": self.clock,
            "TimestampEnd": ,
            # Again, also add process description for this step
            **self.process_description.loc[next_step_id].to_dict(),
        }

    def walk_through_process(self):
        """Process layout and corresponding methods

        Case.__init__()     > START
            |
        do_current_step()   > START completed
            |
        wait_after_step()   > Clock ticks
            |
        go_to_next_step()   > (Step 1) initiated
            |
        do_current_step()   > (Step 1) completed
            |
        wait_after_step()   > Clock ticks
            |
        go_to_next_step()   > (Step 2) initiated
            .
            .
            .
        go_to_next_step()   > END initiated
            |
        do_current_step()   > END completed
            |
        return
        
        """

        # Do all the steps until at END. Simple right?
        while True:
            self.do_current_activity()
            if self.current["ActivityId"] == END:
                return
            self.choose_target_activity()
            self.wait_after_step()
            self.go_to_next_step()


def clean_up(df):
    # Remove START and END rows
    df = df[(df.step_id != START) & (df.step_id != END)]

    # Remove some abundant columns
    df = df.drop(
        columns=[
            "duration",
            "duration_outliers",
            "wait_time",
            "wait_time_outliers",
            "next_possible_steps_id",
            "next_possible_steps_probability",
        ]
    )
    return df


def apply_pafnow_format(df):
    # Change column names
    df.rename(
        columns={
            "case_id": "CaseId",
            "step_id": "ActivityId",
            "step_name": "ActivityName",
            "start_time": "Timestamp",
            "end_time": "TimestampEnd",
        },
        inplace=True,
    )

    # Set date time format to YYYY-MM-DD HH:MM:SS
    df.Timestamp = df.Timestamp.dt.strftime("%Y-%m-%d %H:%M:%S")
    df.TimestampEnd = df.TimestampEnd.dt.strftime("%Y-%m-%d %H:%M:%S")

    # BUG: In pafnow companion! TimestampEnd gives an error. Remove this line when the bug is fixed!
    df.rename(columns={"TimestampEnd": "DontUse"}, inplace=True)

    return df


def stopwatch(func):
    # Read about decorators: https://realpython.com/primer-on-python-decorators/
    def wrapper():
        timer = times.time()
        func()
        print("Done in: %.1f sec" % (times.time() - timer))

    return wrapper


@stopwatch
def main():
    # Parse command line arguments (input filename and approximate number of rows)
    (input_path, approx_rows) = parse_argv(INPUT_PATH, APPROX_ROWS)

    # Read and process the sheets from the input Excel
    print("Reading input from:", input_path)
    (excel_steps, excel_flow) = read_input_file(input_path)
    process_description = merge_flow_into_steps(excel_steps, excel_flow)

    # Note: If you are new to this script, definitely take a look at this dataframe
    # inspect_df(process_description, n=10)

    # We'll drop START and END rows later, so we need to correct (estimation) approx_rows for that
    n_steps = len(excel_steps)
    approx_rows = approx_rows * n_steps / (n_steps - 2)

    # Generate random cases until the dataset is full
    df = pd.DataFrame()
    print("Processing...")
    while len(df) < approx_rows:
        # Start a case and walk through the process
        case = Case(process_description)
        case.walk_through_process()

        # Append the history (completed steps) of this case to our dataset
        df = df.append(case.history)
        print("\r" + str(len(df)), end="")
    print("\n")

    # The basic dataset is done, but we'll do some cleaning up
    df = clean_up(df)

    # Apply format we need for PAFnow
    df = apply_pafnow_format(df)

    inspect_df(df)

    # Save to file
    output_path = splitext(input_path)[0] + ".csv"
    makedirs(output_path, exist_ok=True)
    df.to_csv(output_path, index=False)
    print("Dataset saved in: " + output_path)


if __name__ == "__main__":
    main()
