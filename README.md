# DataAlchemy

DataAlchemy is a comprehensive data pipeline project designed to manage, transform, and load data into a structured Data Warehouse/Lakehouse using Apache Spark. This project emphasizes modular, reusable components for enhanced maintainability and scalability. Built with Python and PySpark, it supports integration with AWS S3 and Delta Lake.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Features](#features)
- [Dataset Structure](#dataset-structure)
- [Technologies Used](#technologies-used)
- [Setup Instructions](#setup-instructions)
- [License](#license)

---
## Data Architecture
---
## Project Overview

DataAlchemy facilitates data engineers in setting up efficient data pipelines, focusing on the transformation of raw data into meaningful insights. It integrates:

- Ingesting, validating, and archiving landing files.
- Populating staging tables.
- Creating dimension and fact tables.
  
### Components:
1. **Initialization Script**:
   The `01_init_db.ipynb` script sets up foundational elements for the project, such as initializing the database schemas.

2. **Data Workflows**:
   Various Jupyter notebooks handle specific tasks for entities like `dim_store`, `dim_customer`, and `fact_sales`.

3. **Utility Layer**:
   Common helper libraries in the `lib/` directory manage tasks like AWS-S3 operations and job logging.

---

## Features

- **Delta Lake Integration**: Ensures ACID transactions on big data workloads.
- **AWS S3 Support**: Manages storage of landing files via S3 buckets.
- **Reusable Notebook Steps**: Modular and scalable Jupyter notebooks.
- **Audit Logging**: Tracks the data processing lifecycle.
- **Symlink Manifest for Athena**: Enables querying through AWS Athena.

---

## Dataset Structure

The project uses significant datasets split into categories:

```plaintext
datasets/
├── Customer/
├── Orders/
├── Product/
└── Store/
```

Detailed ETL flows ingest and process data to populate analytical tables.

---

## Technologies Used

The backbone of **DataAlchemy** features:
- **Languages**:
  - Python
- **Frameworks**:
  - Apache Spark
  - PySpark
- **Storage**:
  - Delta Lake
  - AWS S3
- **Tools & Libraries**:
  - Jupyter Notebooks for task orchestration.
  - Databricks Delta for efficient lakehouse solutions.
  - AWS SDK for S3 integration.

---

## Setup Instructions

### Prerequisites

- Python (>= 3.7)
- Apache Spark
- Access to AWS S3
- Proper environment configuration for `Delta Table`.

### Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/SatishKore11/DataAlchemy.git
   cd DataAlchemy
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure run variables in `run_config.txt`.
4. Use Jupyter Notebooks or .py scripts for specific tasks:
   - Landing Layer: `05_store_landing.py`
   - Development Libraries:
     - `spark_session.py`
     - `aws_s3.py`
     - `utils.py`

5. Run the `01_init_db.ipynb` to initialize schemas.

---

## License

This repository uses the **MIT License**. See the [LICENSE](LICENSE) file for more details.
