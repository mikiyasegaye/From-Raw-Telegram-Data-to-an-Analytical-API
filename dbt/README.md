# DBT Project - Telegram Analytics

This dbt project transforms raw Telegram data into a structured data warehouse optimized for analytics.

## Project Structure

```
dbt/
├── models/
│   ├── staging/
│   │   ├── stg_telegram_messages.sql
│   │   └── _sources.yml
│   └── marts/
│       └── core/
│           ├── dim_channels.sql
│           ├── dim_dates.sql
│           ├── fct_messages.sql
│           └── _schema.yml
├── tests/
│   ├── test_medical_content_consistency.sql
│   └── test_message_engagement_consistency.sql
├── macros/
│   └── medical_content_detection.sql
├── dbt_project.yml
├── profiles.yml
└── README.md
```

## Models Overview

### Staging Models

- **stg_telegram_messages**: Cleans and restructures raw message data
  - Casts data types
  - Extracts key fields from JSON
  - Adds derived fields (message length, medical content detection)
  - Handles null values and data quality issues

### Mart Models (Star Schema)

#### Dimension Tables

- **dim_channels**: Channel information and metrics

  - Channel categories (Medical, Cosmetics, Pharmaceutical)
  - Business sector classification
  - Channel performance metrics
  - Medical content percentage

- **dim_dates**: Time dimension for analysis
  - Date hierarchy (year, month, day, quarter, season)
  - Weekend/weekday flags
  - Daily message metrics
  - Medical content trends

#### Fact Table

- **fct_messages**: Message-level facts with foreign keys
  - Primary key: message_id
  - Foreign keys to dimensions
  - Engagement metrics (views, forwards, replies, reactions)
  - Content classification
  - Business metrics (viral score, engagement score)

## Data Quality Tests

### Built-in Tests

- **not_null**: Ensures critical columns have values
- **unique**: Validates primary keys
- **relationships**: Checks foreign key integrity

### Custom Tests

- **test_medical_content_consistency**: Ensures healthcare channels have reasonable medical content percentage
- **test_message_engagement_consistency**: Validates engagement metrics are logically consistent

## Macros

- **is_medical_content**: Reusable macro for detecting medical/pharmaceutical content in messages

## Setup Instructions

### 1. Install Dependencies

```bash
pip install dbt-postgres
```

### 2. Configure Database Connection

Update `profiles.yml` with your PostgreSQL connection details:

```yaml
telegram_analytics:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: "{{ env_var('DB_USER', 'postgres') }}"
      password: "{{ env_var('DB_PASSWORD', 'password') }}"
      port: "{{ env_var('DB_PORT', 5432) | int }}"
      dbname: "{{ env_var('DB_NAME', 'telegram_analytics') }}"
      schema: "{{ env_var('DB_SCHEMA', 'public') }}"
```

### 3. Load Raw Data

Before running dbt, ensure raw data is loaded:

```bash
python scripts/load_raw_data.py
```

### 4. Run DBT Pipeline

```bash
# Install dependencies
dbt deps

# Debug configuration
dbt debug

# Run models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate

# Serve documentation
dbt docs serve
```

## Using the Python Runner Script

The project includes a Python script for easier dbt management:

```bash
# Run full pipeline
python scripts/run_dbt.py full-pipeline

# Run specific models
python scripts/run_dbt.py run --models staging

# Run tests
python scripts/run_dbt.py test

# Generate documentation
python scripts/run_dbt.py docs-generate
```

## Business Logic

### Medical Content Detection

The project includes sophisticated medical content detection that looks for:

- Medical terms: medicine, drug, pharma, medication, prescription
- Health terms: treatment, symptom, disease, health, medical
- Professional terms: doctor, hospital, clinic, pharmacy
- Beauty/Wellness: cosmetic, beauty, skincare, supplement, vitamin

### Engagement Scoring

- **Engagement Score**: Based on views and content length
- **Viral Score**: Weighted combination of views, forwards, replies, and reactions
- **Content Categories**: Medical Media, Medical Text, Non-Medical Media, Non-Medical Text

### Channel Classification

- **Healthcare**: CheMed123, tikvahpharma (medical supplies and pharmaceuticals)
- **Beauty**: lobelia4cosmetics (cosmetics and beauty products)
- **Other**: General business content

## Data Quality Features

1. **Null Handling**: All models handle null values gracefully
2. **Data Type Casting**: Proper casting of timestamps, numbers, and text
3. **Business Rule Validation**: Custom tests ensure data quality
4. **Documentation**: Comprehensive model and column documentation
5. **Testing**: Built-in and custom tests validate data integrity

## Analytics Capabilities

The transformed data enables:

- **Channel Performance Analysis**: Compare engagement across channels
- **Temporal Analysis**: Track trends over time with detailed date dimensions
- **Content Analysis**: Identify medical vs non-medical content patterns
- **Engagement Analysis**: Understand what drives user engagement
- **Business Intelligence**: Support for dashboards and reporting

## Troubleshooting

### Common Issues

1. **Connection Errors**: Check database credentials in profiles.yml
2. **Model Errors**: Ensure raw data is loaded before running dbt
3. **Test Failures**: Review custom test logic for business rule changes

### Debug Commands

```bash
# Check configuration
dbt debug

# Show model dependencies
dbt ls

# Show model SQL
dbt compile --models stg_telegram_messages
```

## Next Steps

1. **Data Enrichment**: Add YOLO object detection for image analysis
2. **Advanced Analytics**: Create additional mart models for specific use cases
3. **Monitoring**: Set up data quality monitoring and alerting
4. **Documentation**: Enhance documentation with business context and examples
