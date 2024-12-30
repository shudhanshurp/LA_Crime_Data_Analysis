from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, hour, date_format, mean, stddev, abs as spark_abs
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("LACrimeDataTransformation").getOrCreate()

def transform_crime_data():
    raw_data_path = "s3://la-crime-data-sp/raw-data/Crime_Data_from_2020_to_Present.csv"
    df = spark.read.csv(raw_data_path, header=True)
    
    df = df.drop("cross_street", "crime_code_4", "crime_code_3", "crime_code_2")
    
    df = df.fillna({
        'modus_operandi': 'Unknown',
        'victim_sex': 'Unknown',
        'victim_descent': 'Unknown',
        'weapon_code': -1,
        'weapon_description': 'Unknown'
    })
    df = df.na.drop()
    
    df = df.withColumn("date_reported", col("date_reported").cast("timestamp"))
    df = df.withColumn("date_occurred", col("date_occurred").cast("timestamp"))
    
    df = df.withColumn(
        "report_delay",
        when(
            (col("date_reported").isNotNull() & col("date_occurred").isNotNull()),
            (spark_abs((col("date_reported").cast("long") - col("date_occurred").cast("long"))) / (24 * 60 * 60)).cast("int")
        ).otherwise(0)
    )
    
    df = df.withColumn("hour_occurred", hour(col("date_occurred")))
    df = df.withColumn("day_of_week", date_format(col("date_occurred"), "EEEE"))
    
    df = df.filter(col("victim_age") >= 0)
    df = df.withColumn(
        "age_group",
        when(col("victim_age") < 12, "Children")
        .when(col("victim_age").between(12, 19), "Teenagers")
        .when(col("victim_age").between(19, 35), "Young Adults")
        .when(col("victim_age").between(35, 60), "Adults")
        .otherwise("Seniors")
    )
    
    df = df.withColumn(
        "victim_sex",
        when(col("victim_sex") == "M", "Male")
        .when(col("victim_sex") == "F", "Female")
        .when(col("victim_sex") == "X", "Unknown")
        .otherwise("Unknown")
    )
    
    df = df.withColumn(
        "status",
        when(col("status") == "IC", "Investigation Continuing")
        .when(col("status") == "AO", "Adults Only")
        .when(col("status") == "AA", "Adult Arrested")
        .otherwise("Unknown")
    )
    
    df = df.withColumn("latitude", col("latitude").cast(DoubleType()))
    df = df.withColumn("longitude", col("longitude").cast(DoubleType()))
    
    numeric_cols = ["latitude", "longitude", "victim_age"]
    threshold = 3.5
    
    for column_name in numeric_cols:
        stats = df.select(
            mean(col(column_name)).alias("mean"),
            stddev(col(column_name)).alias("stddev")
        ).collect()[0]
        
        col_mean = stats["mean"]
        col_stddev = stats["stddev"]
        
        df = df.withColumn(
            column_name,
            when(
                spark_abs(col(column_name) - col_mean) / col_stddev < threshold,
                col(column_name)
            ).otherwise(None)
        )
    
    df.coalesce(1).write.mode("overwrite").parquet("s3://la-crime-data-sp/transformed-data/cleaned_data.parquet")

transform_crime_data()
