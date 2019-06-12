# Generate process mining data
This *processmine-inator* is a script which generates a randomized dataset to be used for demos or workshops regarding process mining. 

The underlying process should be quantified by filling in the input Excel file. See section **Input** below.

Process mining the output dataset will reveal the underlying process.

## How to use
These instructions assume you've installed Python and git.
1. Clone this repository:  
`git clone https://github.com/eiffelanalytics/generate-process-mining-data`
1. Create a copy of input.example.xlsx and adjust the tables in the sheets to quantify *your process*.
1. Run the script from an IDE or a terminal and optionally supply the name of the *input file* and the desired *approximate number of rows* of the output dataset:  
`python generate_data.py [input_file] [approx_rows]`

## Input
The underlying *process* is described with the Excel input file. Use input.example.xlsx as an example and fill in the following sheets:

* **steps**: A step or activity is equivalent to a *box* in the process mine chart. The order of the list does not matter here.
    * Don't change or remove the rows with start step `START` and end step `END`. The script needs those as a universal start and end point.
* **process_flow**: Each row is equivalent to an *arrow* in the process mine chart. For each `step_id` list the possible `next_step_id`s and corresponding `probability` (value 0 to 1).
    * The sum of probabilities for each unique `step_id` should equal 1.
* **case_properties**: Each unique `property_type` will be an extra column in the output dataset. Each case gets assigned a `value` randomly according to the `probability`.
* **step_properties** (optional): Yet to be implemented.

## Output
The dataset will be stored as a csv in the output directory.


```python
#TODO: Add step_properties.
```
