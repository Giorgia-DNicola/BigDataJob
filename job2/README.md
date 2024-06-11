# Big Data Project: Industry Annual Report

## Project Overview
This project analyzes daily historical stock prices using Hive. The goal is to generate an annual report for each industry, showing the percent change in stock price, the stock with the highest percent increase, and the stock with the highest transaction volume.

## Project Structure
- `data/`: Contains the dataset files.
- `scripts/`: Contains the Hive scripts and helper scripts.
- `results/`: Contains the output and performance comparison files.
- `docs/`: Contains project documentation and presentation.
- `README.md`: Overview of the project and instructions.
- `LICENSE`: Project license.

## Setup
1. Clone the repository.
2. Place the dataset files in the `data/` directory.
3. Update the paths in the Hive scripts if necessary.

## Running the Project
1. Execute the `run_hive_jobs.sh` script to create tables, load data, and generate the report.
    ```bash
    bash scripts/run_hive_jobs.sh
    ```

## Results
- The output of the industry annual report can be found in the `results/` directory.

## Documentation
- Detailed analysis and project report can be found in the `docs/` directory.
