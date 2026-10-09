# Spark Monitoring with Prometheus and Grafana

A containerized data engineering project that processes customer and order data using Apache Spark and monitors the Spark cluster through Prometheus and Grafana.

## Overview

This project demonstrates a batch data pipeline and a monitoring stack using Docker Compose.

The Spark job reads customer and order CSV files, applies data quality filters, joins the datasets, calculates revenue, and aggregates total revenue by country. Prometheus collects exposed Spark metrics, while Grafana visualizes cluster health and resource utilization.

## Architecture

```text
customers.csv ──┐
                ├──> Apache Spark ──> Revenue by Country
orders.csv ─────┘          |
                           v
                    Spark Metrics
                           |
                           v
                       Prometheus
                           |
                           v
                         Grafana
```

## Technology Stack

* **Apache Spark 3.5.6:** Distributed data processing using PySpark.
* **Docker Compose:** Container orchestration for the Spark cluster and monitoring services.
* **Prometheus:** Metrics collection and querying with PromQL.
* **Grafana:** Monitoring dashboards and visualization.
* **Python:** Data transformation and aggregation.

## Project Structure

```text
grafana-lab/
├── data/
│   ├── customers.csv
│   └── orders.csv
├── spark/
│   ├── spark_job.py
│   └── metrics.properties
├── prometheus/
│   └── prometheus.yml
├── grafana/
├── docker-compose.yml
└── README.md
```

## Data Pipeline

The Spark job performs the following steps:

1. Reads the customer and order CSV files.
2. Filters customers to retain records where age is at least 18.
3. Filters orders to retain records with status `Completed` and quantity greater than or equal to 2.
4. Joins valid customers and orders using `customer_id`.
5. Calculates revenue as `quantity * price`.
6. Groups the data by country and calculates total revenue.
7. Sorts the results by total revenue in descending order.

## Monitoring

Prometheus scrapes the configured Spark metrics endpoints. Grafana uses Prometheus as its data source to visualize cluster status and available resource metrics.

Examples of metrics available in this setup include:

* Active Spark workers
* Registered Spark applications
* Waiting Spark applications
* Registered Spark workers
* Worker executors
* Free and used worker cores
* Worker memory metrics

The available metrics depend on the configured Spark endpoints and whether the relevant application or executor is currently running.

**Note:** Spark job counts (running, completed, and failed) and CPU utilization require suitable instrumentation or additional metrics sources if they are not exposed by the current configuration.

## Prerequisites

Install the following software:

* Docker Desktop
* Git
* A GitHub account

Make sure Docker Desktop is running before starting the containers.

## Run the Project

### 1. Start the monitoring stack and Spark cluster

```powershell
docker compose up -d
```

### 2. Check the container status

```powershell
docker compose ps
```

### 3. Run the Spark job

```powershell
docker compose run --rm spark-job
```

### 4. View the logs

```powershell
docker compose logs spark-master
docker compose logs spark-worker
docker compose logs prometheus
docker compose logs grafana
```

### 5. Open the web interfaces

| Service         | URL                   |
| --------------- | --------------------- |
| Spark Master UI | http://localhost:8080 |
| Spark Worker UI | http://localhost:8081 |
| Prometheus      | http://localhost:9090 |
| Grafana         | http://localhost:3000 |

Grafana's default credentials are commonly `admin` / `admin` unless changed in the configuration. Use the credentials configured for your deployment.

## Prometheus Configuration

Spark's metrics configuration is defined in:

```text
spark/metrics.properties
```

Prometheus scrape targets are configured in:

```text
prometheus/prometheus.yml
```

To inspect available metric names, open Prometheus and query:

```promql
{__name__=~".+"}
```

To stop the containers:

```powershell
docker compose down
```

## Future Improvements

* Add explicit running, completed, and failed Spark job counters.
* Collect container CPU and memory utilization.
* Expand the Grafana dashboard with alerts and additional operational metrics.
* Add automated tests for the data transformation logic.

## Author

**Abdullah Fathy**

GitHub: [abdullah-fat7y](https://github.com/abdullah-fat7y)
