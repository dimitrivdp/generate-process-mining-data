TODO for next release:
* Remove all formating from Excel input files. Nu is het niet duidelijk bij fouten met . of , voor decimalen
* Doe alle input files in /input/ en schrijf output met "output" in naam en naar dezelfde subfolder in /output/.
* Maak 1 example input.xlsx & output.csv
* `wait_time` moet naar `processflow` want gaat over edges, niet over nodes.

# Generate a process mining dataset
This script generates a custom, randomized dataset which can be analyzed with process mining. Useful for demos or workshops! 

## How to set up
These instructions assume you've installed and set up [Python](https:/www.anaconda.com/distribution/#download-section) and [git](https://gitforwindows.org/).
1. Clone this repository (run the command in a terminal or Anaconda Prompt):  
`git clone https://github.com/eiffelanalytics/generate-process-mining-data`
1. Enter the new directory: `cd generate-process-mining-data`
1. Test it:
`python generate_data.py examples/process_123.xlsx 1000`
1. If all went well the script produced a dataset, based on the Excel file `examples/process_123.xlsx` of approximately `1000` rows in the output folder.

## How to use
1. Create a new
Now adjust the Excel file to describe your custom process and run the script again.

## How to fill the input Excel file
You have to define your *process* in an input file. See the example folder for different scenarios.
> Tip: To make things easier, draw the process on paper before creating the input file.

* Sheet `steps`: Basically a list of all steps of the process. The order of the list does not matter here.
    * Required columns are `step_id` and `duration`.
    * Optional columns are `step_name`, `wait_time`, `duration_outliers` and `wait_time_outliers`. Also custom attributes, such as `Employee` can be added as a column.
    > Don't change or remove the rows with `START` and `END`. The script needs those as general start and end points.
* Sheet `process_flow`: Defines the process flow from step to next step. Specify the possible next steps and probabilities (value 0 to 1).
    * Layout is fixed with columns `step_id`, `next_step_id` and `probability`.
    > Verify that the sum of `probability` for each unique `step_id` equals 1.
