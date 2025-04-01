Assignment 1 - Apache Iceberg and PyIceberg:

Create a daily partitioned table in your username's schema for MAANG stock prices from Polygon
Make a new audit branch for that table with PyIceberg create_branch function
In the script that adds a new daily summary file to the daily partition every day

Assignment 2 - AirFlow:

Create a daily partitioned table in Iceberg that tracks the price of MAANG stocks (and others if you want)
Create a script that loads the data from polygon into the Iceberg staging table
Run data quality checks on the data making sure everything looks legit
Exchange the data from the staging into production
Write a cumulation script to pull the price data into arrays (storing the rolling last 7 elements) incrementally for faster computation


Assignment 3 - Working with DataFrames in Databricks
Load the tabular.dataexpert.yello_taxi table into a Spark DataFrame
Display the DataFrame and print its schema
Apply transformations such as filtering, sorting, and column selection
Perform narrow transformations (select(), filter(), map())
Perform wide transformations (groupBy(), reduceByKey(), join())
Execute actions to count rows, retrieve the first five rows, and collect all data
Load and process data using SQL queries
Read Drug_Use_Data_from_Selected_Hospitals.csv using schema inference and a user-defined schema
Compare schema differences and analyze the results










