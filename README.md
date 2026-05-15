# AUTO-SCHEMA-PIPELINE

### Instead of spending hours figuring out what a CSV file is telling you, this pipeline will **automatically** detect the schema, and generate **AI-powered** analysis in seconds

## Problem Statement

When you get a new CSV file, you lose hours just trying to figure out what the data is telling you. Whether it’s sales logs,server logs or something else, a developer has to manually check headers, infer data types, and map out the schema before they can even start. This "cold start" is the most time-consuming part because you’re stuck writing boilerplate code and exploratory SQL just to see what’s inside.

Auto-Schema-Pipeline automates this entire manual process. Once you upload a CSV, the pipeline programmatically figures out the schema and uses Gemini to generate specific SQL queries based on the data it sees. Instead of you writing queries by hand, the results appear immediately on a dashboard and a PDF. It replaces the "understanding" phase with an automated loop, so you go from a raw file to clear results in seconds.

## Demo

description and screenshots with demo video

## What This Project Does

- Detects the schema of any uploaded CSV — column names, data types, 
  and structure — with no manual configuration
- Dynamically creates a PostgreSQL table and loads the data, 
  named and tracked per file
- Sends schema and sample rows to Google Gemini, which generates 
  targeted SQL queries specific to that dataset
- Validates each generated query before execution — bad queries 
  are discarded, not crashed on
- Executes valid queries against PostgreSQL and stores results 
  as a structured JSON, unique to each pipeline run
- Automatically selects the appropriate chart type per result 
  and renders visualisations
- Surfaces everything on a Streamlit dashboard with per-run 
  navigation and downloadable PDF report

## System Architecture
![Architecture Diagram](docs/architecture.png)

The user uploads any CSV through the dashboard, triggering the pipeline. 
The dataset is cleaned, schema is inferred, and a PostgreSQL table is 
dynamically created and populated. Gemini then receives the schema and 
sample rows, generates targeted SQL queries, which are validated, executed 
against the database, and stored as a structured JSON unique to that run.
Results are surfaced on the Streamlit dashboard and a downloadable PDF. 
If any critical stage fails, the run is marked as failed and an appropriate 
error is shown on the dashboard.

## Pipeline flow

1. **Data Ingestion** — The user-uploaded CSV is fetched from storage 
   and loaded into a Pandas DataFrame for processing.

2. **Cleaning & Preprocessing** — Duplicates are removed and missing 
   values are filled to ensure the dataset is accurate before it touches 
   the database.

3. **Schema Detection** — Each DataFrame column is analysed, and its 
   type is mapped to the equivalent PostgreSQL data type.

4. **Database Load** — A PostgreSQL table is dynamically created from 
   the detected schema and populated with the cleaned data. The run is 
   logged to the pipeline_runs table.

5. **AI Query Generation** — The schema and sample rows are sent to 
   Google Gemini, which identifies the dataset type and generates 
   targeted SQL queries for meaningful analysis.

6. **Query Validation** — Each AI-generated query is vetted. Harmful 
   commands are blocked and structurally invalid queries are discarded 
   before execution.

7. **Query Execution** — Valid queries are executed against PostgreSQL. 
   Results are captured and stored as a structured JSON, unique to 
   that pipeline run.

8. **Report Generation** — The JSON is parsed and formatted into a 
   structured, downloadable PDF report.

9. **Dashboard Rendering** — Results are surfaced on an interactive 
   Streamlit dashboard. If the run succeeded, the user can explore 
   insights and download the PDF. If a critical stage failed, an 
   appropriate error is displayed.

## Tech Stack

| Category       | Technology              |
|----------------|-------------------------|
| Language       | Python                  |
| Data           | Pandas                  |
| Database       | PostgreSQL, SQLAlchemy  |
| AI Layer       | Google Gemini           |
| Visualisation  | Matplotlib              |
| Dashboard      | Streamlit               |
| Reporting      | FPDF2                   |
| Config         | python-dotenv           |

## Folder Tree

add image

## Local Setup and requirements

para

## Engineering challenges and Solutions

para

## Future Enhancements

list

## Author conatct

add links

## License

optional