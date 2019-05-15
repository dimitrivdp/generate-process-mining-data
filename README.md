# Generate process mining data
A script which generates a randomized dataset of cases which follow a certain process. Process mining this dataset should reveal the underlying process.

## How to use
These instructions assume you've installed Python and git.
1. Clone this repository: `git clone https://github.com/eiffelanalytics/generate-process-mining-data`
    * This will create a directory called generate-process-mining-data in your currect working directory.
1. Create a copy of input.example.xlsx and adjust the tables in the sheets to match *your process*.
1. Run the script from an IDE or a terminal and optionally supply the name of the *input file* and the desired *approximate number of rows* of the output dataset: `python generate_data.py [input_file] [approx_rows]`
    * Run `python generate_data --help` for more info and the default values.

## Input
In order to generate the dataset, the *process* should be described through an Excel input file. Use input.example.xlsx as an example. Fill in the following sheets:

* **step**: List all the process activities here. Give each step an *unique id*. The order of the list does not matter here.
    * Don't change or remove the rows with start step `0` and end step `N`. The script needs those as a universal start and end point.
* **process_flow**: Describe the flow of cases through the process. For each `step_id` list the possible `next_step_id`s and corresponding `probability` (value 0 to 1).
    * The sum of probabilities for each step should equal 1.
* **case_property** (optional): Yet to be implemented.
* **step_property** (optional): Yet to be implemented.

## Output
The dataset will be stored as a csv in the output directory.


```python
#TODO: Add end_time, case_properties and step_properties.
```
