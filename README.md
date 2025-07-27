# OpenWeatherMap ETL Pipeline

This project demonstrates an end-to-end ETL pipeline using data from the OpenWeatherMap API. It is developed as part of a data engineering portfolio project to showcase capabilities in API integration, data transformation, cloud data warehousing, and workflow orchestration.

## Project Overview

The objective of this project is to extract current weather data from selected global cities using the OpenWeatherMap API, transform and enrich the data for analytical purposes, store the cleaned data in Google BigQuery, and finally visualize the insights via an interactive dashboard built with Looker Studio. The project is fully automated using Prefect Cloud and scheduled to run every two days.

## Features

- Extract weather data (temperature, humidity, wind, etc.) from OpenWeatherMap API
- Clean and enrich data with derived fields such as temperature category, hour, and day
- Store the data in Google BigQuery for downstream analytics
- Build an interactive dashboard with filters for time, city, and temperature category
- Automate pipeline runs using Prefect Cloud and GitHub integration

## Project Structure

```
/OWM_ETL_PROJECT/
├── config/
│   ├── .env                    # Environment variables (not committed)
│   ├── city_list.csv          # List of city IDs used in extraction
│   └── service_account.json   # GCP credentials (not committed)
├── etl/
│   ├── extract.py             # Extract weather data from API
│   ├── transform.py           # Clean and enrich data
│   └── load.py                # Load data to BigQuery
├── etl_main.py                # Prefect-deployable flow script
├── etl_deployment.py          # Deployment configuration for Prefect Cloud
├── main.py                    # Local runner for testing the pipeline
├── requirements.txt           # Python dependencies
└── README.md                  # Project documentation
```

## Technologies Used

- Python (requests, pandas, dotenv)
- Google BigQuery (via pandas-gbq)
- Prefect Cloud (workflow orchestration)
- GitHub (deployment source)
- Looker Studio (data visualization)
- OpenWeatherMap API

## Setup Instructions

1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/owm-etl-project.git
   cd owm-etl-project
   ```

2. Create and activate a virtual environment.

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Add your `.env` file under `config/`:
   ```
   OWM_API_KEY=your_openweathermap_api_key
   PROJECT_ID=your_gcp_project_id
   TABLE_ID=your_dataset.your_table
   GOOGLE_APPLICATION_CREDENTIALS=config/your-service-account.json
   ```

5. Run locally:
   ```bash
   python main.py
   ```

6. Or deploy to Prefect Cloud via:
   ```bash
   python etl_deployment.py
   ```

## Dashboard

The interactive dashboard is published using Looker Studio. It includes temperature trends, weather condition distribution, and filtering by city and temperature category.

**Dashboard Link:** [View Dashboard](https://lookerstudio.google.com/reporting/73ba2017-5e17-4642-a49d-9968069af385)

## License

This project is developed for educational purposes. Please do not publish sensitive credentials.