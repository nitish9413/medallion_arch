# Medallion Architecture ETL Demo with Prefect, Delta Lake, and Polars

This repository is a **mini-project** that demonstrates an ETL (Extract, Transform, Load) pipeline using [Prefect](https://www.prefect.io/) as the workflow orchestrator, [Polars](https://www.pola.rs/) for fast DataFrame operations, and [Delta Lake](https://delta.io/) for ACID-compliant data storage. The project is structured around the **medallion architecture** (bronze, silver, gold layers), focusing on the bronze layer as a starting point.

## Project Overview

- **Goal:** Showcase a simple, modern ETL pipeline for batch data ingestion and management using open-source tools.
- **Architecture:** Medallion (Bronze/Silver/Gold) — this repo implements the **bronze layer**.
- **Technologies:**
  - **Prefect**: Orchestrates the ETL flow and task dependencies.
  - **Polars**: High-performance DataFrame library for reading and processing CSV data.
  - **Delta Lake**: Provides ACID transactions and scalable metadata handling for data lakes.

## How It Works

1. **Raw Data Ingestion:**
   - Raw CSV files (e.g., Binance BTCUSDT OHLCV) are placed in the `raw/` directory.
2. **Bronze Layer ETL:**
   - The ETL flow (see `main.py`) reads all CSVs from `raw/`.
   - Data is loaded into a Delta Lake table in the `bronze/` directory using Polars.
   - Delta Lake ensures ACID compliance and efficient upserts/merges.
3. **Orchestration:**
   - Prefect manages the flow, logging, and error handling for each step.

## File Structure

```
medallion_arch/
├── main.py              # Main ETL flow (Prefect, Polars, Delta Lake)
├── pyproject.toml       # Project dependencies
├── README.md            # Project documentation
├── raw/                 # Raw input CSV files
└── bronze/              # Delta Lake bronze layer output
```

## Getting Started

### 1. Install Dependencies

This project requires **Python 3.12+**. Install dependencies with:

```sh
pip install -r requirements.txt
# or, if using pyproject.toml:
pip install .
```

### 2. Prepare Data

Place your raw CSV files in the `raw/` directory. Example files are already included.

### 3. Run the ETL Flow

```sh
python main.py
```

- The flow will initialize a Delta Lake table in `bronze/` (if not present).
- All CSVs in `raw/` will be ingested and merged into the bronze Delta table.

## Key Concepts

- **Medallion Architecture:**
  - **Bronze:** Raw, ingested data (this repo)
  - **Silver:** Cleaned, enriched data (future extension)
  - **Gold:** Business-level aggregates (future extension)
- **Delta Lake:** Enables ACID transactions, scalable metadata, and time travel for data lakes.
- **Polars:** Fast, memory-efficient DataFrame operations.
- **Prefect:** Modern Python workflow orchestration.

## Extending This Project

- Add **Silver** and **Gold** layer flows for further data processing and analytics.
- Integrate with cloud storage (S3, Azure, GCS) for scalable data lakes.
- Add unit tests and CI/CD for production readiness.

## References
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Prefect Documentation](https://docs.prefect.io/)
- [Polars Documentation](https://docs.pola.rs/)
- [Delta Lake Documentation](https://docs.delta.io/)

---

*This project is for educational and demonstration purposes.*
