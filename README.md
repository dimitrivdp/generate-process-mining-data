# Generate process mining data
A script which generates a randomized dataset. Process mining this dataset should reveal the underlying process.

## How to use
1. Install git and Python.
1. Clone this repository: `git clone https://github.com/eiffelanalytics/generate-process-mining-data`
1. Create a copy of input.sample.xlsx and adjust the tables in the sheets to match *your process*.
1. Adjust the parameters in the .py file using a text editor of your choice.
    * See Script parameters below.
1. Run the script from an IDE or a terminal: `python generate-data.py`

## Input
In order to generate the dataset, the *process* is described through an Excel input file. Use input.sample.xlsx as an example. Fill in the following sheets:

* **activity**: List all the process activities here. Give each activity an *unique id*. The order of the list does not matter here.
    * Don't change or remove the rows with start activity `0` and end activity `N`.
* **process_flow**: This sheets describes the flow of cases throught the process. For each `activity_id` list the possible `next_activity_id`s and corresponding `probability` (value 0 to 1).
    * The sum of probabilities for each activity should equal 1.
* **case_property** (optional): Yet to be implemented.
* **activity_property** (optional): Yet to be implemented.

## Script parameters
The script uses some parameters:

* `approx_max_rows`: The approximate number of rows of the output dataset.
* `input_file`: The filename and path of the input file, e.g. input.xlsx.

Adjust these parameters in the script.

## Output
The dataset will be stored in output/ as a csv with the following columns: case_id, activity_id, activity_name and start_time.

TODO: Add end_time, case_properties and activity_properties.

