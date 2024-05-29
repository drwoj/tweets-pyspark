# Twitter Posts Analysis with PySpark

This project uses Apache Spark (using PySpark) to analyze Twitter posts (Covid, Grammys and financial tweets). The application is Dockerized and can be run using Docker Compose.

## Prerequisites

- Docker

## Setup

Clone the repository to your local machine.

```bash
git clone git@github.com:drwoj/tweets-pyspark.git
```

## Running the Application

1. Build the Docker images:

```bash
docker-compose build
```

2. Run the Docker containers:

```bash
docker-compose up -d
```

3. Submit the Spark application:

```bash
docker-compose exec spark-master spark-submit --master spark://spark-master:7077 src/main.py
```

## Stopping the Application

To stop the application and remove the containers defined in the `docker-compose.yml` file, run:

```bash
docker-compose down
```

## Accessing the Application

You will be able to access it through a Spark WEB UI. The port (9090) specified in `docker-compose.yml` will be exposed on your host machine, so you can access S[park Master by navigating to `localhost:9090` in your web browser.
