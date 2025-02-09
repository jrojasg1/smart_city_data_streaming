# Real-Time Data Processing System

This repository contains a real-time data processing system that integrates Confluent Kafka, Apache Spark, and AWS services such as Glue, Redshift, and Athena to efficiently process and analyze streaming data.



## System Overview

The system is designed to process real-time data from multiple sources, including:
- **Vehicle Information**
- **GPS Information**
- **Camera Information**
- **Weather Information**
- **Emergency Information**

These data streams are ingested into **Confluent Kafka**, where they are processed using **Apache Spark Streaming** and stored in AWS for further analytics.

## Architecture
![image](https://github.com/user-attachments/assets/e290c69d-a4fb-4a5d-9436-3c2eb73175a5)

### **1. Data Ingestion**
- Data from various sources is pushed into Kafka topics.
- **Apache Zookeeper** is used to manage Kafka brokers.
- Data producers send information in real-time.

### **2. Stream Processing**
- **Apache Spark** (running in a distributed cluster with master and worker nodes) consumes data from Kafka.
- Spark processes and transforms the data in real-time.
- Processed data is forwarded to AWS storage for further transformations.

### **3. Data Storage & Processing**
- Data is initially stored in **AWS S3 (Raw Storage)**.
- **AWS Glue Crawlers** scan and catalog the data.
- The data is transformed and stored in **AWS S3 (Transformed Storage)**.
- AWS Glue jobs prepare the data for analytics.

### **4. Data Analytics & Querying**
- Transformed data is made available in **Amazon Redshift** for querying and reporting.
- **Amazon Athena** is used for serverless querying of the stored data.
- **AWS IAM** manages access permissions for secure data handling.

## Deployment

To deploy the system, follow these steps:

1. **Set up Kafka Cluster:**
   - Deploy Apache Kafka and Zookeeper using Docker.
   - Create required Kafka topics for each data source.
   
2. **Deploy Spark Streaming Application:**
   - Configure Spark to consume data from Kafka.
   - Process and transform streaming data.
   - Store processed data in AWS S3.
   
3. **Configure AWS Services:**
   - Set up AWS Glue Crawlers and Data Catalog.
   - Define ETL jobs to process raw data.
   - Load transformed data into Amazon Redshift.
   - Use Athena for querying.

## Requirements

- **Docker** (for Kafka & Zookeeper setup)
- **Apache Kafka & Zookeeper**
- **Apache Spark** (Streaming enabled)
- **AWS S3, Glue, Redshift, Athena**
- **IAM roles and policies for secure access**

## Running the System

1. Start the Kafka cluster:
   ```sh
   docker-compose up -d
   ```
2. Run the Spark Streaming job:
   ```sh
   spark-submit --master spark://<master-node>:7077 main.py
   ```
3. Configure AWS Glue Crawlers and execute them to catalog the data.
4. Run AWS Glue ETL jobs to transform and load data.
5. Query the processed data using Redshift or Athena.

## Conclusion

This project provides a scalable and real-time data processing pipeline leveraging Kafka, Spark, and AWS services. It enables efficient ingestion, transformation, storage, and analysis of streaming data for insights and decision-making.

