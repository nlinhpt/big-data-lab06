from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, date_format, desc, broadcast
from pyspark.sql.window import Window


# =====================================================
# Spark Session
# =====================================================
def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("Pagila Analysis - Lesson 6")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# =====================================================
# Load tables from PostgreSQL (Pagila schema)
# =====================================================
def load_tables(spark):
    jdbc_url = "jdbc:postgresql://postgres-db:5432/analytics"
    jdbc_props = {
        "user": "spark_user",
        "password": "spark_password",
        "driver": "org.postgresql.Driver"
    }

    payment = spark.read.jdbc(jdbc_url, "payment", properties=jdbc_props)
    rental = spark.read.jdbc(jdbc_url, "rental", properties=jdbc_props)
    inventory = spark.read.jdbc(jdbc_url, "inventory", properties=jdbc_props)
    film = spark.read.jdbc(jdbc_url, "film", properties=jdbc_props)
    film_category = spark.read.jdbc(jdbc_url, "film_category", properties=jdbc_props)
    category = spark.read.jdbc(jdbc_url, "category", properties=jdbc_props)

    return payment, rental, inventory, film, film_category, category




# =====================================================
# QUESTION 1
# Monthly revenue by film category
# =====================================================
def compute_monthly_revenue(payment, rental, inventory, film, film_category, category):
    return (
        payment
        .join(rental, "rental_id")
        .join(inventory, "inventory_id")
        .join(film, "film_id")
        .join(film_category, "film_id")
        .join(category, "category_id")
        .groupBy(
            date_format(col("payment_date"), "yyyy-MM").alias("month"),
            col("name").alias("category")
        )
        .agg(sum("amount").alias("monthly_revenue"))
        .orderBy("month", desc("monthly_revenue"))
    )


# =====================================================
# QUESTION 2
# Customer Lifetime Value (CLV)
# =====================================================
def compute_clv(payment):
    return (
        payment
        .groupBy("customer_id")
        .agg(sum("amount").alias("clv"))
        .orderBy(desc("clv"))
    )


# =====================================================
# QUESTION 3
# Top 1% customers generating 80% revenue
# =====================================================
def compute_top_customers(clv_df):
    total_revenue = clv_df.agg(sum("clv")).collect()[0][0]

    window = (
        Window
        .orderBy(desc("clv"))
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return (
        clv_df
        .withColumn("cumulative_revenue", sum("clv").over(window))
        .withColumn(
            "revenue_ratio",
            col("cumulative_revenue") / total_revenue
        )
        .filter(col("revenue_ratio") <= 0.8)
    )


# =====================================================
# QUESTION 4
# Partitioning strategy
# =====================================================
def partition_payment_by_date(payment):
    (
        payment
        .withColumn("year", date_format(col("payment_date"), "yyyy"))
        .withColumn("month", date_format(col("payment_date"), "MM"))
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet("/opt/spark-data/payment_partitioned_by_date")
    )


# =====================================================
# MAIN
# =====================================================
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    print("\n================ QUESTION 1 =================")
    print("Using Spark, compute monthly revenue by film category.\n")
    print(
        "We join payment → rental → inventory → film → film_category → category. "
        "Revenue is computed using SUM(amount) and grouped by month and category.\n"
    )

    payment, rental, inventory, film, film_category, category = load_tables(spark)

    monthly_revenue = compute_monthly_revenue(
        payment, rental, inventory, film, film_category, category
    )
    monthly_revenue.show(truncate=False)

    print("\n================ QUESTION 2 =================")
    print("Define customer lifetime value (CLV) using Spark.\n")
    print(
        "CLV is defined as the total monetary value contributed by a customer over time. "
        "It is calculated by summing all payment amounts per customer.\n"
    )

    clv_df = compute_clv(payment)
    clv_df.show(truncate=False)

    print("\n================ QUESTION 3 =================")
    print("Identify the top 1% of customers generating 80% of revenue.\n")
    print(
        "Customers are sorted by CLV in descending order. "
        "Using a window function, cumulative revenue is computed and customers "
        "contributing up to 80% of total revenue are selected.\n"
    )

    top_customers = compute_top_customers(clv_df)
    top_customers.show(truncate=False)

    print("\n================ QUESTION 4 =================")
    print("Propose a partitioning strategy for the payment table.\n")
    print(
        "• Partition by date (year, month): BEST choice. Optimizes time-based analytics "
        "and incremental data loads.\n"
        "• Partition by store: Useful for store-level reporting but may lead to skew.\n"
        "• Partition by customer: NOT recommended due to high cardinality and many small files.\n"
    )

    partition_payment_by_date(payment)
    print("Payment table written as Parquet partitioned by year and month.\n")

    print("\n================ QUESTION 5 =================")
    print("Optimizing slow joins at scale.\n")
    print(
        "• Join order optimization: Join large fact tables first, broadcast small dimension tables.\n"
        "• Indexing strategies: Create indexes on foreign keys in PostgreSQL (rental_id, inventory_id, film_id).\n"
        "• Caching / Materialized views: Cache small dimensions in Spark and precompute "
        "fact-to-dimension joins when reused.\n"
    )

    optimized_join = (
        payment
        .join(rental, "rental_id")
        .join(inventory, "inventory_id")
        .join(film, "film_id")
        .join(broadcast(film_category), "film_id")
        .join(broadcast(category), "category_id")
    )

    print("Optimized join using broadcast completed successfully.")
    print("\n========== END OF ANALYSIS ==========\n")

    spark.stop()


if __name__ == "__main__":
    main()
