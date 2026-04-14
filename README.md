# STEDI Human Balance Analytics

## Project Overview
Built a data lakehouse solution on AWS for the STEDI Step Trainer team to curate sensor data for machine learning.

## Architecture
Landing Zone -> Trusted Zone -> Curated Zone

## AWS Services Used
- AWS S3: Data storage
- AWS Glue: ETL jobs using PySpark
- AWS Athena: SQL queries on S3 data

## Files
- customer_landing.sql: DDL for customer landing table
- accelerometer_landing.sql: DDL for accelerometer landing table
- step_trainer_landing.sql: DDL for step trainer landing table
- customer_landing_to_trusted.py: Filter customers who consented to research
- accelerometer_landing_to_trusted.py: Filter accelerometer data for consented customers
- customer_trusted_to_curated.py: Customers with accelerometer data
- step_trainer_trusted.py: Step trainer data for curated customers
- machine_learning_curated.py: Aggregated ML training dataset

## Row Counts
Landing: customer=956, accelerometer=81273, step_trainer=28680
Trusted: customer=482, accelerometer=40981, step_trainer=14460
Curated: customer=482, machine_learning=43681
