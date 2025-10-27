# Airflow ETL Project

This project implements an ETL pipeline using Apache Airflow to transfer data from MySQL to PostgreSQL.

## Project Structure
```
.
├── dags/                  # DAG files
├── logs/                  # Airflow logs
├── plugins/              # Airflow plugins
├── init-scripts/         # Database initialization scripts
├── Dockerfile            # Airflow custom image
├── docker-compose.yaml   # Docker services configuration
├── requirements.txt      # Python dependencies
└── setup.sh             # Setup script
```

## Prerequisites
- Docker
- Docker Compose

## Setup Instructions

1. Clone the repository:
```bash
git clone <repository-url>
cd <project-directory>
```

2. Run the setup script:
```bash
chmod +x setup.sh
./setup.sh
```

3. Build and start the containers:
```bash
docker-compose build
docker-compose up -d
```

4. Initialize Airflow (first time only):
```bash
docker-compose run airflow-webserver airflow db init
docker-compose run airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

5. Access the services:
- Airflow UI: http://localhost:8080 (username: admin, password: admin)
- MySQL: localhost:3306
- PostgreSQL: localhost:5432

## DAG Description
The ETL pipeline (`mysql_to_postgres_etl`) runs hourly and:
1. Tests database connections
2. Creates target table if not exists
3. Extracts data from MySQL for the previous hour
4. Transforms the data
5. Loads data into PostgreSQL
6. Performs data quality checks

## Data Persistence
Data is persisted using Docker volumes:
- `postgres-data`: PostgreSQL data
- `mysql-data`: MySQL data
- Local mount for logs, DAGs, and plugins

## Environment Variables
The following environment variables are set in docker-compose.yaml:
- Database credentials
- Airflow configuration
- Connection strings

## Troubleshooting
1. Check container logs:
```bash
docker-compose logs -f
```

2. Reset the environment:
```bash
docker-compose down -v  # Warning: This will delete all data
docker-compose up -d
```

3. Access container shell:
```bash
docker-compose exec airflow-webserver bash
```
