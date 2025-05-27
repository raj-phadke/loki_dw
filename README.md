# 🐱‍👤 loki_dw

loki_dw repository provides an extensible framework for building Data Warehouse (DW) dimension transformations using Apache Spark. It includes concrete implementations for Slowly Changing Dimensions (SCD) Type 1 and Type 2, utilities for handling DataFrame transformations, and validation mechanisms via configuration classes.

---

## 🚀 Overview

The codebase defines abstract base classes and concrete implementations for creating DW entities, specifically SCD Type 1 and Type 2 dimensions, following best practices for data warehousing and Change Data Capture (CDC).

### 🔑 Key Components

- **DwSCDType1DimensionTransform**: Implements SCD Type 1 dimension transformations with support for adding metadata columns like `dw_created_at`.

- **DwSCDType2DimensionTransform**: Implements SCD Type 2 dimension transformations including full CDC logic, metadata management, and handling of active/inactive record states.

- **BaseDWTransform**: Abstract base class providing common infrastructure such as logging, Spark session management, and utility instantiation.

- **DwTransformUtils**: A utility class with static methods for common Spark DataFrame operations such as column existence checks, renaming and casting columns, creating hash columns, and rearranging columns.

- **Configuration Classes** (`DwDimensionConfig` and others): Provide strong validation for transformation parameters, including constraints such as CDC parameters only being valid for SCD2 subtypes.

---

## ✨ Features

- **SCD Type 1 Transformation**: Adds necessary metadata for SCD Type 1 and handles simple overwrite scenarios.

- **SCD Type 2 Transformation**:
  - Performs CDC between source and target datasets to identify inserts, updates, and deletions.
  - Adds SCD Type 2 specific metadata columns such as `dw_start_date`, `dw_end_date`, and `is_active`.
  - Handles expiration of dropped source records based on configuration.
  - Uses hash-based comparisons for efficient CDC.

- **Reusable Utility Functions**:
  - ✅ Column existence validation
  - 🔐 Hash column generation with SHA-256
  - 🔄 Dynamic renaming and casting of columns
  - 📋 Column rearrangement and lowercasing

- **Validation and Safety**:
  - 🚫 Ensures critical configuration fields are present and consistent.
  - ⚠️ Validates CDC parameters are only used with appropriate SCD types.

---

## 📦 Getting Started

### 🔧 Prerequisites

- Apache Spark 3.x
- Python 3.8+
- PySpark library installed
- Your project environment should have access to the necessary configuration and utility
