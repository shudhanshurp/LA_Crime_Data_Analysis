# LA Crime Data Analysis Pipeline

## Overview

This repository documents a crime data analysis pipeline designed to ingest, process, and visualize crime data. The primary goals are to identify crime hotspots, analyze trends over time, and correlate crime incidents with victim demographics. The solution architecture incorporates AWS services, Apache Airflow for orchestration, dbt for data transformation, and Power BI for visualization.

---

## Data Ingestion

The data ingestion process begins by extracting crime data from an online CSV source using a shell script executed via an Apache Airflow DAG.

### Key Technologies:

-   **AWS S3**: Storage layer for raw crime data and logs.
-   **AWS EC2**: Hosts the Airflow service.
-   **AWS EMR**: Manages big data frameworks, including Apache Spark for large-scale data processing.

### Airflow Setup:

The DAG, `crime_data_emr_pipeline`, orchestrates the entire lifecycle:

1. Creation of the EMR cluster.
2. Execution of data extraction and transformation steps.
3. Cluster termination.

### Extraction Step:

-   **Script**: `ingest.sh`
-   **Function**: Downloads a CSV file from a GitHub repository and uploads it to an S3 bucket.

---

## Data Transformation

Apache Spark, running on an EMR cluster, handles data transformation.

### Transformation Script:

-   **File**: `la_crime_data_transformation.py`
-   **Details**:
    -   Cleans and transforms the data by:
        -   Dropping unnecessary columns.
        -   Filling missing values.
        -   Converting data types.
        -   Calculating new fields, such as report delay.
        -   Categorizing data by time of occurrence and victim demographics.

The script is submitted to the EMR cluster as a step using the Airflow `EmrAddStepsOperator`.

---

## Data Modeling with dbt

After transformation, the data is loaded into **DuckDB** for querying and analytics. **dbt (data build tool)** is used to model the data into actionable insights.

### dbt Models:

1. **Source Data Model**:

    - Standardizes raw data.
    - Ensures correct formatting (e.g., date formats, consistent categorical entries for crime types).

2. **Base Model**:

    - Aggregates crime data to the area level.
    - Summarizes crime counts and geographical data for each area.
    - Calculates total crime numbers and median coordinates for spatial analysis.

3. **Top10Crime Model**:
    - Identifies the ten most frequent crime types.
    - Sorts crimes by occurrence frequency for prioritized attention.

---

## Visualization with Power BI

The transformed and modeled data is visualized through Power BI dashboards to provide actionable insights.

### Dashboards:

-   **Crime Hotspots Identification**: Maps and charts displaying crime concentration and trends by area.

    ![Crime Hotspots](/analysis_dashboard_img/crime_analysis-1.png)

-   **Crime Trends Analysis**: Temporal visualizations (e.g., monthly, daily, and hourly patterns).

    ![Crime Trends](/analysis_dashboard_img/crime_analysis-2.png)

-   **Crime Type Distribution**: Breakdown of crime types and area distributions.

    ![Crime Type Distribution](/analysis_dashboard_img/crime_analysis-3.png)

-   **Victim Demographics**: Correlations between victim age/sex and crime incidents.

    ![Victim Demographics](/analysis_dashboard_img/crime_analysis-4.png)

-   **Response Times and Outcomes**: Metrics for law enforcement response times and investigation outcomes.

    ![Response Times](/analysis_dashboard_img/crime_analysis-5.png)
