# Companies House Accounts Pipeline

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue.svg)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12%2B-blue.svg)](https://www.postgresql.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A robust ETL pipeline for downloading, parsing, and storing Companies House accounts data from monthly iXBRL filings into PostgreSQL. Processes ~300,000 files per batch with parallel processing, intelligent write strategies, comprehensive error handling, and automated verification.

---

## 📋 Table of Contents
- [Project Overview](#project-overview)
- [Schema Design](#schema-design)
- [Architecture](#architecture)
- [Component Breakdown](#component-breakdown)
- [Trade-offs & Design Decisions](#trade-offs--design-decisions)
- [Verification](#verification)
- [Setup Instructions](#setup-instructions)
- [Usage](#usage)

---

## 🚀 Project Overview

This pipeline automates the ingestion of Companies House monthly accounts data:

| Stage | Description |
|-------|-------------|
| **Download** | Fetches monthly ZIP files (1.8 GB) from Companies House |
| **Extract** | Unzips ~300,000 iXBRL HTML files |
| **Parse** | Extracts structured data using ixbrlparse library |
| **Process** | Parallel processing with configurable workers |
| **Store** | Writes to PostgreSQL with appropriate write strategies |
| **Verify** | Automatic data quality checks after completion |

---

## 📊 Schema Design

### Database Tables

| Table | Strategy | Unique Constraint | Purpose |
|-------|----------|-------------------|---------|
| **financials** | APPEND ONLY | None | Preserves full time series of financial metrics across multiple filing periods |
| **directors** | UPSERT | (company_number, director_name) | Maintains latest director list only |
| **reports** | UPSERT | (company_number, section) | Stores latest narrative sections (strategic report, directors report, etc.) |
| **metadata** | UPSERT | (company_number, field) | Keeps latest company details (name, address, auditor, dormancy status) |
| **processed_files** | INSERT | source_file (PK) | Tracks which files have been successfully processed (idempotency) |
| **ch_pipeline_runs** | INSERT | ch_upload (PK) | Monitors pipeline execution per month (status, counts, timing) |

### Why These Choices?

#### Financials - APPEND ONLY
- **Reason**: Need full time series to track trends over multiple years
- **Example**: A company's revenue for 2023, 2024, 2025 should all be preserved
- **No unique constraint** allows multiple entries per company per metric across different periods

#### Directors/Reports/Metadata - UPSERT
- **Reason**: Only the latest filing matters for current data
- **Conflict handling**: `ON CONFLICT DO UPDATE` with latest values
- **Indexes**: `(company_number, unique_field)` for efficient upsert operations

---

