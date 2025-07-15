# From-Raw-Telegram-Data-to-an-Analytical-API

An end-to-end data pipeline for Telegram, leveraging dbt for transformation, Dagster for orchestration, and YOLOv8 for data enrichment.

## Project Overview

This project builds a robust data platform to generate insights about Ethiopian medical businesses using data scraped from public Telegram channels. The platform implements a modern ELT (Extract, Load, Transform) framework with the following components:

- **Data Extraction**: Telegram API scraping using Telethon
- **Data Storage**: PostgreSQL data warehouse with dimensional modeling
- **Data Transformation**: dbt for data cleaning and modeling
- **Data Enrichment**: YOLOv8 object detection on images
- **Analytics API**: FastAPI for exposing insights
- **Orchestration**: Dagster for pipeline management

## Current Status

**Task 0 - Project Setup & Environment Management** **COMPLETE**
**Task 1 - Data Scraping and Collection** **COMPLETE**
**Task 2 - Data Modeling and Transformation** **COMPLETE**
**Task 3 - Data Enrichment (YOLOv8 Medical Detection):** **COMPLETE**
**Task 4 - FastAPI Analytical API** **COMPLETE**

### Completed Tasks:

**Task 0 - Project Setup & Environment Management:**

- **Environment Management**: Docker, requirements.txt, environment variables
- **Database Setup**: PostgreSQL with initialization scripts
- **Project Structure**: Professional directory organization
- **Configuration**: Centralized settings with validation
- **Basic API**: FastAPI application with health checks
- **Documentation**: Comprehensive README and setup instructions

**Task 1 - Data Scraping and Collection:**

- **Telegram Scraper**: Extracts data from Ethiopian medical channels
- **Data Lake**: Partitioned JSON storage with date-based organization
- **Image Download**: Automatic media download for YOLO analysis
- **Rate Limiting**: Robust error handling and retry mechanisms
- **Logging System**: Comprehensive logging with file rotation
- **Database Integration**: Seamless loading into PostgreSQL
- **Command-Line Interface**: Easy-to-use CLI with multiple options
- **Test Suite**: Complete validation of all components

**Task 2 - Data Modeling and Transformation:**

- **Raw Data Loading**: Script to load JSON files into PostgreSQL raw schema
- **dbt Project Setup**: Complete dbt configuration with PostgreSQL adapter
- **Staging Models**: Clean and restructure raw data with data type casting
- **Star Schema Implementation**:
  - `dim_channels`: Channel information and metrics
  - `dim_dates`: Time dimension with daily metrics
  - `fct_messages`: Message-level facts with foreign keys
- **Data Quality**: Built-in and custom tests for validation
- **Business Logic**: Medical content detection and engagement scoring
- **Documentation**: Comprehensive model documentation and schema definitions

**Task 3 - Data Enrichment (YOLOv8 Medical Detection):**
- Implemented YOLOv8-based detection pipeline for medical equipment in images
- Automated loading of detection results into the database
- Integrated detection data into dbt models and analytics
- Documented the process and pipeline steps

**Task 4 - FastAPI Analytical API:**
- **API Structure**: Modular FastAPI application with APIRouter
- **Analytical Endpoints**: Business-focused endpoints for insights
- **Data Validation**: Pydantic schemas for request/response validation
- **Database Integration**: SQLAlchemy with connection pooling
- **CRUD Operations**: Complex queries against dbt models
- **Error Handling**: Comprehensive error handling and logging
- **API Documentation**: Auto-generated OpenAPI/Swagger docs

### Ready for next tasks:

- **Task 5**: Dagster orchestration ready

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Telegram      │    │   Data Lake     │    │   PostgreSQL    │
│   Channels      │───▶│   (Raw JSON)    │───▶│   Data Warehouse│
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
                                                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   FastAPI       │◀───│   dbt Models    │◀───│   YOLO Object   │
│   Analytics API │    │   (Star Schema) │    │   Detection     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Telegram API credentials

### 1. Environment Setup

1. Clone the repository:

```bash
git clone <repository-url>
cd From-Raw-Telegram-Data-to-an-Analytical-API
```

2. Create your environment file:

```bash
cp env.example .env
```

3. Configure your `.env` file with your credentials:

```bash
# Telegram API Configuration
TELEGRAM_API_ID=your_telegram_api_id_here
TELEGRAM_API_HASH=your_telegram_api_hash_here
TELEGRAM_PHONE=your_phone_number_here

# Database Configuration
POSTGRES_PASSWORD=your_secure_password_here
```

4. Install Python dependencies:

```bash
pip install -r requirements.txt
```

### 2. Start the Application

```bash
# Start all services
docker-compose up -d

# Or start specific services
docker-compose up postgres app
```

### 3. Access Services

- **FastAPI Application**: http://localhost:8000
- **FastAPI Documentation**: http://localhost:8000/docs
- **FastAPI ReDoc**: http://localhost:8000/redoc
- **Dagster UI**: http://localhost:3000
- **PostgreSQL Database**: localhost:5432

### 4. Run the FastAPI Application

```bash
# Activate virtual environment
source venv/bin/activate

# Run the FastAPI application
python -m app.main

# Or using uvicorn directly
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## Project Structure

```
├── app/                           # Main application code
│   ├── api/                      # FastAPI endpoints **COMPLETE**
│   │   ├── main.py              # API router with analytical endpoints
│   │   ├── schemas.py           # Pydantic models for validation
│   │   ├── database.py          # Database connection management
│   │   ├── crud.py              # CRUD operations for analytics
│   │   └── __init__.py
│   ├── core/                     # Core configuration and utilities ✅
│   │   ├── config.py            # Environment and settings management
│   │   └── __init__.py
│   ├── scrapers/                 # Telegram scraping modules **COMPLETE**
│   │   ├── telegram_scraper.py  # Main scraper implementation
│   │   ├── data_loader.py       # Database loading functionality
│   │   ├── run_scraper.py       # Command-line interface
│   │   └── test_scraper.py      # Test suite
│   ├── models/                   # Data models (ready for Task 2)
│   ├── utils/                    # Utility functions
│   │   └── logger.py            # Logging system
│   ├── main.py                   # FastAPI application **COMPLETE**
│   ├── setup_database.py         # Database initialization
│   └── __init__.py
├── data/                         # Data storage
│   ├── raw/telegram_messages/   # Raw scraped data **ACTIVE**
│   ├── processed/               # Processed data (ready for Task 2)
│   └── images/                  # Downloaded images **ACTIVE**
├── dbt/                         # dbt transformation models **COMPLETE**
├── dagster/                     # Dagster orchestration (ready for Task 5)
├── tests/                       # Test files
├── logs/                        # Application logs **ACTIVE**
├── requirements.txt              # Python dependencies
├── requirements-minimal.txt      # Minimal dependencies for Task 1
├── Dockerfile                   # Application container
├── docker-compose.yml           # Multi-service orchestration
├── init.sql                    # Database initialization
├── env.example                 # Environment template
├── telegram_session.session     # Telegram session file
└── README.md                   # Project documentation
```

## Configuration

### Environment Variables

| Variable               | Description                | Default        |
| ---------------------- | -------------------------- | -------------- |
| `TELEGRAM_API_ID`      | Telegram API ID            | Required       |
| `TELEGRAM_API_HASH`    | Telegram API Hash          | Required       |
| `TELEGRAM_PHONE`       | Phone number for Telegram  | Required       |
| `POSTGRES_PASSWORD`    | PostgreSQL password        | `password`     |
| `DATABASE_URL`         | Database connection string | Auto-generated |
| `YOLO_MODEL`           | YOLO model file            | `yolov8n.pt`   |
| `CONFIDENCE_THRESHOLD` | YOLO confidence threshold  | `0.5`          |

### Telegram API Setup

1. Visit https://my.telegram.org/apps
2. Create a new application
3. Copy the API ID and API Hash
4. Add them to your `.env` file

## Data Pipeline

### 1. Data Extraction (Task 1) **COMPLETE**

- Scrapes data from Ethiopian medical Telegram channels
- Stores raw data in partitioned JSON files (`data/raw/telegram_messages/YYYY-MM-DD/channel_name.json`)
- Downloads images for object detection (`data/images/`)
- Robust logging and error handling with file rotation
- Rate limiting and retry mechanisms for API stability
- Command-line interface with multiple options
- Database integration for seamless data loading
- Comprehensive test suite with 4/4 tests passing

### 2. Data Modeling (Task 2) **COMPLETE**

- **Raw Data Loading**: Script to load JSON files into PostgreSQL raw schema
- **dbt Project Setup**: Complete dbt configuration with PostgreSQL adapter
- **Staging Models**: Clean and restructure raw data with data type casting
- **Star Schema Implementation**:
  - `dim_channels`: Channel information and metrics
  - `dim_dates`: Time dimension with daily metrics
  - `fct_messages`: Message-level facts with foreign keys
- **Data Quality**: Built-in and custom tests for validation
- **Business Logic**: Medical content detection and engagement scoring
- **Documentation**: Comprehensive model documentation and schema definitions

### 3. Data Enrichment (Task 3) **COMPLETE**

- YOLOv8 object detection on images
- Enriches data with detected objects
- Links visual content to messages

#### Task 3: Medical Image Detection Pipeline

**Overview:**
We implemented a YOLOv8-based pipeline to detect medical equipment in images from Ethiopian medical businesses.

**Steps:**
1. **Detection:**
   - Run `scripts/medical_detection.py` to process images in `data/images/` and output results to `data/processed/medical_detections.csv`.
2. **Load Results:**
   - Use `scripts/load_detection_results.py` to load detection results into the database.
3. **dbt Models:**
   - Run `dbt run` and `dbt test` to transform and validate detection data.
4. **Documentation:**
   - Generate docs with `dbt docs generate`.

**Notes:**
- If no detections are found, the pipeline still runs end-to-end for reproducibility.
- To retrain or improve detection, update the YOLO model and rerun the detection script.

### 4. Analytics API (Task 4) **COMPLETE**

- **Top Products Endpoint**: `/api/reports/top-products` - Most frequently mentioned medical products
- **Channel Activity**: `/api/channels/{channel_name}/activity` - Posting activity and engagement metrics
- **Message Search**: `/api/search/messages` - Search messages by keywords
- **Channel Summary**: `/api/channels` - Summary statistics for all channels
- **Medical Content Stats**: `/api/reports/medical-content` - Medical vs non-medical content analysis
- **Data Validation**: Pydantic schemas ensure data integrity
- **Query Optimization**: Efficient SQL queries against dbt models
- **API Documentation**: Auto-generated OpenAPI/Swagger documentation

#### Available API Endpoints:

**Analytics Endpoints:**
- `GET /api/reports/top-products?limit=10&days=30` - Get top mentioned medical products
- `GET /api/channels/{channel_name}/activity?days=30` - Channel activity analysis
- `GET /api/channels` - All channels summary
- `GET /api/reports/medical-content?days=30` - Medical content statistics

**Search Endpoints:**
- `GET /api/search/messages?query=paracetamol&limit=20` - Search messages by keyword

**Health Endpoints:**
- `GET /` - Main application health check
- `GET /health` - Detailed health status

**Documentation:**
- `GET /docs` - Interactive API documentation (Swagger UI)
- `GET /redoc` - Alternative API documentation (ReDoc)

#### Running the API:

```bash
# Start the FastAPI application
source venv/bin/activate
python -m app.main

# Access the API
curl http://localhost:8000/

# View API documentation
open http://localhost:8000/docs
```

### 5. Orchestration (Task 5)

- Dagster for pipeline orchestration
- Scheduled data processing
- Monitoring and alerting

## Testing

```bash
# Run tests
pytest tests/

# Run with coverage
pytest --cov=app tests/
```

## API Testing Examples

### Test the FastAPI Application

```bash
# Start the API
source venv/bin/activate
python -m app.main

# Test health endpoint
curl http://localhost:8000/

# Test API documentation
open http://localhost:8000/docs
```

### Example API Calls

```bash
# Get top mentioned medical products
curl "http://localhost:8000/api/reports/top-products?limit=5&days=30"

# Get channel activity for a specific channel
curl "http://localhost:8000/api/channels/CheMed123/activity?days=30"

# Search for messages containing "paracetamol"
curl "http://localhost:8000/api/search/messages?query=paracetamol&limit=10"

# Get summary of all channels
curl "http://localhost:8000/api/channels"

# Get medical content statistics
curl "http://localhost:8000/api/reports/medical-content?days=30"
```

### Python API Testing

```python
import requests

# Base URL
base_url = "http://localhost:8000"

# Test health check
response = requests.get(f"{base_url}/")
print(response.json())

# Get top products
response = requests.get(f"{base_url}/api/reports/top-products", 
                       params={"limit": 5, "days": 30})
products = response.json()
print(f"Found {len(products)} top products")

# Search messages
response = requests.get(f"{base_url}/api/search/messages", 
                       params={"query": "vitamin", "limit": 5})
messages = response.json()
print(f"Found {len(messages)} messages containing 'vitamin'")
```

## Business Questions Answered

The platform answers key business questions:

1. **Top Products**: Most frequently mentioned medical products across Ethiopian channels
2. **Price Analysis**: Product price variations across different channels and regions
3. **Visual Content**: Channels with most image content for product analysis
4. **Trends**: Daily/weekly posting volume trends for market insights
5. **Channel Performance**: Comparative analysis of CheMed123, lobelia4cosmetics, and tikvahpharma

## API Endpoints

- `GET /api/reports/top-products?limit=10` - Top mentioned products
- `GET /api/channels/{channel_name}/activity` - Channel activity
- `GET /api/search/messages?query=paracetamol` - Message search
- `GET /api/channels` - All channels summary
- `GET /api/reports/medical-content` - Medical content statistics
- `GET /health` - Health check

## Development

### Local Development

```bash
# Install dependencies
pip install -r requirements-minimal.txt

# Setup database
python -m app.setup_database

# Test scraper configuration
python -m app.scrapers.test_scraper

# Run scraper (with options)
python -m app.scrapers.run_scraper --limit 500 --load-data

# Run FastAPI
uvicorn app.main:app --reload

# Run Dagster
dagster dev
```

### Quick Start

```bash
# 1. Activate virtual environment
source venv/bin/activate

# 2. Test the scraper
python -m app.scrapers.test_scraper

# 3. Run a small test scrape
python -m app.scrapers.run_scraper --limit 10 --dry-run --verbose

# 4. Run actual scraping
python -m app.scrapers.run_scraper --limit 100 --load-data
```

### Scraper Usage

```bash
# Test the scraper configuration
python -m app.scrapers.test_scraper

# Basic scraping (1000 messages per channel)
python -m app.scrapers.run_scraper

# Scrape with custom limit
python -m app.scrapers.run_scraper --limit 500

# Scrape specific channels
python -m app.scrapers.run_scraper --channels CheMed123 lobelia4cosmetics tikvahpharma

# Load data into database after scraping
python -m app.scrapers.run_scraper --load-data

# Dry run (test without scraping)
python -m app.scrapers.run_scraper --dry-run --verbose

# Scrape and load with custom limit
python -m app.scrapers.run_scraper --limit 100 --load-data --verbose
```

### Supported Channels

The scraper is configured to extract data from the following Ethiopian medical Telegram channels:

- **CheMed123** (477 subscribers): የመድሀኒትና የህክምና እቃዎች አፋላጊ እና አቅራቢ ድርጅት
- **lobelia4cosmetics** (16,868 subscribers): American and Canadian Genuine products
- **tikvahpharma** (90,154 members): Pharma Consultant, Sales, Marketing, Promotion

### Data Lake Structure

```
data/
├── raw/telegram_messages/
│   ├── 2024-01-15/
│   │   ├── CheMed123.json
│   │   ├── lobelia4cosmetics.json
│   │   └── tikvahpharma.json
│   └── ...
└── images/
    ├── CheMed123_12345.jpg
    ├── lobelia4cosmetics_67890.jpg
    └── ...
```

### Database Management

```bash
# Run dbt models
cd dbt
dbt run

# Generate documentation
dbt docs generate
dbt docs serve
```

## Logging

Logs are stored in the `logs/` directory with different levels:

- Application logs: `logs/app.log`
- Scraping logs: `logs/scraping.log`
- Error logs: `logs/errors.log`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

This project is licensed under the MIT License.

## Support

For support and questions:

- Create an issue in the repository
- Contact the development team
- Check the documentation

## Task Progress

- **Task 0**: Project Setup & Environment Management - **COMPLETE**
- **Task 1**: Data Scraping and Collection - **COMPLETE**
- **Task 2**: Data Modeling and Transformation - **COMPLETE**
- **Task 3**: Data Enrichment with YOLO - **COMPLETE**
- **Task 4**: Analytics API with FastAPI - **COMPLETE**
- **Task 5**: Pipeline Orchestration with Dagster - **READY TO START**

---

**Built by the Kara Solutions Data Engineering Team**
