# Generate a process mining dataset
This script generates a custom, randomized dataset which can be analyzed with process mining. Useful for demos or workshops! 

## How to use
These instructions assume you've installed and set up [Python](https:/www.anaconda.com/distribution/#download-section) and [git](https://gitforwindows.org/).
1. Clone this repository (run the command in a terminal or Anaconda Prompt):  
`git clone https://github.com/eiffelanalytics/generate-process-mining-data`
1. Test it:
`python generate_data.py examples/process_123.xlsx 1000`
1. Copy and edit one of the example Excel files so it matches *your process*.
1. Run the script from a terminal:  
`python generate_data.py [input_file] [approx_rows]`  
    * `input_file` should be the path and filename to your input Excel file
    * The script tries to build a dataset with `approx_rows` number of rows

## Input Excel file
You have to define your *process* in an input file. See the example folder for different scenarios.
> Tip: To make things easier, draw the process on paper before creating the input file.

* Sheet `steps`: Basically a list of all steps of the process. The order of the list does not matter here.
    * Required columns are `step_id` and `duration`.
    * Optional columns are `step_name`, `wait_time`, `duration_outliers` and `wait_time_outliers`. Also custom attributes, such as `Employee` can be added as a column.
    > Don't change or remove the rows with `START` and `END`. The script needs those as general start and end points.
* Sheet `process_flow`: Defines the process flow from step to next step. Specify the possible next steps and probabilities (value 0 to 1).
    * Layout is fixed with columns `step_id`, `next_step_id` and `probability`.
    > Verify that the sum of `probability` for each unique `step_id` equals 1.

## Output CSV file
The dataset will be stored as a csv in the output directory.
