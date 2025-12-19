# Weather Batch ETL Pipeline

Automated ETL pipeline to extract weather data from OpenWeatherMap, transform it using Pandas, and load it into PostgreSQL. Orchestrated with **Apache Airflow** and containerized with **Docker**.

## Architecture
**Extract** (API) ➔ **Transform** (Pandas) ➔ **Load** (PostgreSQL)
*(Scheduled with Airflow)*
<img width="1477" height="431" alt="Screenshot 2025-12-19 105848" src="https://github.com/user-attachments/assets/fc6426c9-6b87-4ce2-8443-9895dd7545c9" />


## Tech Stack
- **Language:** Python 3.9
- **Orchestration:** Apache Airflow 2.7
- **Containerization:** Docker & Docker Compose
- **Database:** PostgreSQL
- **Data Processing:** Pandas

## How to Run
1. Clone this repository
2. Create `.env` file based on `.env.example`
   ```bash
   cp .env.example .env
   # Edit .env and put your API Key & DB Credentials
3. Build and Run Docker
   ```bash
   docker-compose up -d --build
4. Access Airflow UI at http://localhost:8080
5. Enable the weather_etl_pipeline DAG

## Database Schema
The pipeline creates a table weather_data with columns:
city, temperature, humidity, timestamp, etc.
