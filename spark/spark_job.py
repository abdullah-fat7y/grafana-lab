"""
Spark Job: Customer Orders Revenue Analysis
============================================

Purpose
-------
This PySpark application reads customers and orders CSV files,
cleans and filters the data according to the assignment requirements,
calculates revenue for valid orders, and produces total revenue
for each country.

Processing requirements
-----------------------
1. Keep customers where age >= 18.
2. Keep orders where status = "Completed".
3. Keep orders where quantity >= 2.
4. Join valid orders with valid customers using customer_id.
5. Calculate order revenue:
       revenue = quantity * price
6. Calculate total revenue for each country.
7. Sort countries by total revenue in descending order.

Input files
-----------
/opt/spark-data/customers.csv
/opt/spark-data/orders.csv

Output
------
The final result is displayed in the Spark application logs.

Expected columns
----------------
Final result:
    country
    total_revenue

Monitoring
----------
The Spark application is designed to run inside Docker and expose
Spark metrics for Prometheus monitoring.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

CUSTOMERS_PATH = "/opt/spark-data/customers.csv"
ORDERS_PATH = "/opt/spark-data/orders.csv"


def create_spark_session():
    """
    Create and return the SparkSession used by the application.

    SparkSession is the main entry point for working with DataFrames
    and Spark functionality in PySpark.
    """

    print("[INFO] Creating SparkSession...")

    spark = (
        SparkSession.builder
        .appName("CustomerOrdersRevenue")
        .getOrCreate()
    )

    print("[INFO] SparkSession created successfully.")

    return spark

def read_input_data(spark):
    """
    Read customers.csv and orders.csv into Spark DataFrames.

    The CSV files contain headers, and inferSchema allows Spark to
    automatically determine basic column data types.
    """

    print("[INFO] Reading customers.csv...")

    customers = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(CUSTOMERS_PATH)
    )

    print("[INFO] Customers data loaded.")

    print("[INFO] Reading orders.csv...")

    orders = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(ORDERS_PATH)
    )

    print("[INFO] Orders data loaded.")

    return customers, orders


def clean_customers(customers):
    """
    Keep only adult customers.

    Assignment requirement:
        age >= 18
    """

    print("[INFO] Filtering customers: age >= 18...")

    cleaned_customers = customers.filter(
        col("age") >= 18
    )

    print("[INFO] Customer filtering completed.")

    return cleaned_customers


def clean_orders(orders):
    """
    Filter orders according to the assignment requirements.

    Requirements:
        1. status must be "Completed"
        2. quantity must be >= 2
    """

    print("[INFO] Filtering orders: status = Completed...")

    completed_orders = orders.filter(
        col("status") == "Completed"
    )

    print("[INFO] Filtering orders: quantity >= 2...")

    cleaned_orders = completed_orders.filter(
        col("quantity") >= 2
    )

    print("[INFO] Order filtering completed.")

    return cleaned_orders


def calculate_revenue(cleaned_customers, cleaned_orders):
    """
    Join customers and orders and calculate revenue.

    The join is performed using customer_id.

    An inner join is used so that only orders belonging to valid
    customers are included in the final result.

    Revenue formula:
        revenue = quantity * price
    """

    print("[INFO] Joining customers and orders...")

    joined_data = cleaned_orders.join(
        cleaned_customers,
        on="customer_id",
        how="inner"
    )

    print("[INFO] Join completed.")

    print("[INFO] Calculating order revenue...")

    revenue_data = joined_data.withColumn(
        "revenue",
        col("quantity") * col("price")
    )

    print("[INFO] Revenue calculation completed.")

    return revenue_data


def aggregate_revenue(revenue_data):
    """
    Calculate total revenue for every country.

    The result contains:
        country
        total_revenue

    Results are sorted from highest revenue to lowest revenue.
    """

    print("[INFO] Calculating total revenue by country...")

    result = (
        revenue_data
        .groupBy("country")
        .agg(
            spark_sum("revenue").alias("total_revenue")
        )
        .orderBy(
            col("total_revenue").desc()
        )
    )

    print("[INFO] Revenue aggregation completed.")

    return result


def display_result(result):
    """
    Display the final result in the Spark logs.
    """

    print("")
    print("=" * 60)
    print("FINAL RESULT: TOTAL REVENUE BY COUNTRY")
    print("=" * 60)

    result.show(
        truncate=False
    )

    print("=" * 60)
    print("[INFO] Final result displayed successfully.")


def main():
    """
    Main application workflow.

    The workflow is intentionally kept simple:

        1. Create SparkSession
        2. Read input data
        3. Clean customers
        4. Clean orders
        5. Calculate revenue
        6. Aggregate by country
        7. Display result
        8. Stop Spark
    """

    print("")
    print("=" * 60)
    print("STARTING SPARK CUSTOMER ORDERS JOB")
    print("=" * 60)

    spark = None

    try:
        # -------------------------------------------------------------
        # 1. Create SparkSession
        # -------------------------------------------------------------

        spark = create_spark_session()
        # Reduce Spark's internal logs while keeping warnings and errors visible.
        spark.sparkContext.setLogLevel("WARN")

        # -------------------------------------------------------------
        # 2. Read input data
        # -------------------------------------------------------------

        customers, orders = read_input_data(spark)

        # -------------------------------------------------------------
        # 3. Clean customers
        # -------------------------------------------------------------

        cleaned_customers = clean_customers(customers)

        # -------------------------------------------------------------
        # 4. Clean orders
        # -------------------------------------------------------------

        cleaned_orders = clean_orders(orders)

        # -------------------------------------------------------------
        # 5. Calculate order revenue
        # -------------------------------------------------------------

        revenue_data = calculate_revenue(
            cleaned_customers,
            cleaned_orders
        )

        # -------------------------------------------------------------
        # 6. Aggregate revenue by country
        # -------------------------------------------------------------

        result = aggregate_revenue(revenue_data)

        # -------------------------------------------------------------
        # 7. Display final result
        # -------------------------------------------------------------

        display_result(result)

        # -------------------------------------------------------------
        # Force Spark to execute the complete computation.
        #
        # Spark transformations are lazy. collect() is an action,
        # so it forces Spark to actually execute the DataFrame plan.
        # -------------------------------------------------------------

        # print("[INFO] Executing Spark computation...")

        # result.collect()

        # print("[INFO] Spark computation completed successfully.")

    except Exception as exc:
        # Print a simple error message before Spark exits.
        print("")
        print("[ERROR] Spark job failed.")
        print(f"[ERROR] {exc}")

        # Re-raise the exception so Spark returns a failure status.
        raise

    finally:
        # -------------------------------------------------------------
        # Always stop Spark when the application finishes.
        # -------------------------------------------------------------

        if spark is not None:
            print("[INFO] Stopping SparkSession...")
            spark.stop()
            print("[INFO] SparkSession stopped.")

    print("")
    print("=" * 60)
    print("SPARK JOB FINISHED")
    print("=" * 60)


# ---------------------------------------------------------------------
# Python application entry point
# ---------------------------------------------------------------------

if __name__ == "__main__":
    main()