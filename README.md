# Renewable Energy Tracking System

This project is a simple data engineering pipeline built using Python and SQLite.

It processes renewable energy data from a CSV file, performs cleaning and transformation, stores the processed data in a database, and runs analytical queries to generate insights.

## Project Structure

renewable-energy-tracker/

- data/
  - Energy_project_data.csv

- database/
  - energy.db

- src/
  - extract.py
  - transform.py
  - load.py
  - queries.py
  - analyse.py
  - utils.py

- main.py
- requirements.txt
- README.md

## Features

- Reads raw CSV data
- Cleans and transforms data (handling missing values, type conversion)
- Creates new calculated columns (profit, efficiency, growth, etc.)
- Stores processed data into SQLite database
- Runs multiple SQL queries:
  - State-wise energy analysis
  - Monthly and yearly trends
  - Growth analysis
  - Efficiency and performance analysis
  - Profit and cost analysis
- Saves query results into a text file

## Technologies Used

- Python
- Pandas
- NumPy
- SQLite3

## How to Run

1. Install dependencies:

   pip install -r requirements.txt

2. Run the pipeline:

   python main.py

## Output

- Processed data stored in:
  
  database/energy.db

- Query results saved in:

  output.txt

## Notes

- Some invalid rows in the dataset are skipped during loading.
- The project is designed for learning basic data engineering concepts such as ETL and SQL analysis.