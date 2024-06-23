# Home Sales Analysis with PySpark - Module 22 Challenge

## Project Overview

In this challenge, you'll use your knowledge of SparkSQL to determine key metrics about home sales data. You'll use Spark to create temporary views, partition the data, cache and uncache a temporary table, and verify that the table has been uncached.

## Dataset

The dataset used in this project is `home_sales_revised.csv`, which is downloaded from the following URL: https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv


## PySpark Analysis

### Steps Performed

1. **Initialisation and Setup:**
   - Initialise Spark and create a SparkSession.
   - Download and read the CSV file into a Spark DataFrame.

2. **Data Preparation:**
   - Create a temporary table `home_sales`.

3. **SQL Queries:**
   - Calculate the average price for four-bedroom houses sold each year:
     ```
     | year | avg_price  |
     |------|------------|
     | 2019 | 300263.70  |
     | 2020 | 298353.78  |
     | 2021 | 301819.44  |
     | 2022 | 296363.88  |
     ```

   - Calculate the average price of homes with three bedrooms and three bathrooms for each year they were built:
     ```
     | year_built | avg_price  |
     |------------|------------|
     | 2010       | 292859.62  |
     | 2011       | 291117.47  |
     | 2012       | 293683.19  |
     | 2013       | 295962.27  |
     | 2014       | 290852.27  |
     | 2015       | 288770.30  |
     | 2016       | 292055.07  |
     | 2017       | 292676.79  |
     ```

   - Calculate the average price of homes with specific criteria (three bedrooms, three bathrooms, two floors, and >= 2000 sqft):
     ```
     | year_built | avg_price  |
     |------------|------------|
     | 2010       | 285010.22  |
     | 2011       | 276553.81  |
     | 2012       | 307539.97  |
     | 2013       | 303676.79  |
     | 2014       | 298264.72  |
     | 2015       | 297609.97  |
     | 2016       | 293965.10  |
     | 2017       | 280317.58  |
     ```

   - Calculate the average price of homes per view rating with an average price >= $350,000 and determine runtime:
     ```
     | view | avg_price      |
     |------|----------------|
     | 91   | 1137372.73     |
     | 97   | 1129040.15     |
     | 84   | 1112733.13     |
     | 75   | 1114042.94     |
     | 89   | 1107839.15     |
     | 78   | 1080649.37     |
     | 77   | 1076285.56     |
     | 87   | 1072285.2      |
     | 86   | 1070444.25     |
     | 82   | 1063498.0      |
     ```

4. **Caching:**
   - Cache the `home_sales` table and verify caching.

5. **Performance Comparison:**
   - Uncached runtime: `0.602045822143555 seconds`
   - Cached runtime: `0.6052045822143555 seconds`
   - Parquet runtime: `0.3876931667327881 seconds`

6. **Partitioning:**
   - Partition the data by `date_built` and save it as Parquet files.
   - Create a temporary table from the Parquet data.
   - Rerun the last query on the Parquet table and measure runtime.

7. **Uncaching:**
   - Uncache the `home_sales` table and verify.

## Conclusion

The analysis showed how caching and partitioning can impact the performance of Spark SQL queries. Caching slightly increased the runtime due to the small dataset size, while partitioning the data and using Parquet files significantly improved the runtime.

## How to Run

1. **Environment Setup:**
   - Ensure you have PySpark installed. You can install it using `pip install pyspark`.

2. **Run the Notebook:**
   - Open the `Home_Sales.ipynb` notebook in Jupyter Notebook or JupyterLab.
   - Run all cells to execute the analysis.

## References

- Apache Software Foundation. 2024. "Apache Spark - Python API Documentation." Accessed June 10, 2024. [https://spark.apache.org/docs/latest/api/python/](https://spark.apache.org/docs/latest/api/python/).
- **Data**: 2U Data Curriculum Team. 2024. "Home Sales Dataset." Accessed June 10, 2024. [https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv](https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.2/22-big-data/home_sales_revised.csv).
