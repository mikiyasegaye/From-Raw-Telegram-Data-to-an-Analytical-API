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

**Task 0 - Project Setup & Environment Management** is complete! The foundation is ready for the remaining tasks:

- **Environment Management**: Docker, requirements.txt, environment variables
- **Database Setup**: PostgreSQL with initialization scripts
- **Project Structure**: Professional directory organization
- **Configuration**: Centralized settings with validation
- **Basic API**: FastAPI application with health checks
- **Documentation**: Comprehensive README and setup instructions

**Ready for next tasks:**

- **Task 1**: Telegram scraping infrastructure ready
- **Task 2**: Database and dbt setup ready
- **Task 3**: YOLO integration ready
- **Task 4**: FastAPI foundation ready
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
- **Dagster UI**: http://localhost:3000
- **PostgreSQL Database**: localhost:5432

## Project Structure

```
├── app/                           # Main application code
│   ├── api/                      # FastAPI endpoints (ready for Task 4)
│   ├── core/                     # Core configuration and utilities ✅
│   │   ├── config.py            # Environment and settings management
│   │   └── __init__.py
│   ├── scrapers/                 # Telegram scraping modules (ready for Task 1)
│   ├── models/                   # Data models (ready for Task 2)
│   ├── utils/                    # Utility functions
│   ├── main.py                   # FastAPI application ✅
│   ├── setup_database.py         # Database initialization ✅
│   └── __init__.py
├── data/                         # Data storage
│   ├── raw/telegram_messages/   # Raw scraped data (ready for Task 1)
│   ├── processed/               # Processed data (ready for Task 2)
│   └── images/                  # Downloaded images (ready for Task 3)
├── dbt/                         # dbt transformation models (ready for Task 2)
├── dagster/                     # Dagster orchestration (ready for Task 5)
├── tests/                       # Test files
├── logs/                        # Application logs
├── requirements.txt              # Python dependencies ✅
├── Dockerfile                   # Application container ✅
├── docker-compose.yml           # Multi-service orchestration ✅
├── init.sql                    # Database initialization ✅
├── env.example                 # Environment template ✅
└── README.md                   # Project documentation ✅
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

### 1. Data Extraction (Task 1)

- Scrapes data from Ethiopian medical Telegram channels
- Stores raw data in partitioned JSON files
- Downloads images for object detection

### 2. Data Modeling (Task 2)

- Loads raw data into PostgreSQL
- Uses dbt for data transformation
- Implements star schema for analytics

### 3. Data Enrichment (Task 3)

- YOLOv8 object detection on images
- Enriches data with detected objects
- Links visual content to messages

### 4. Analytics API (Task 4)

- FastAPI endpoints for business insights
- Query optimization for analytical queries
- Data validation with Pydantic

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

## Business Questions Answered

The platform answers key business questions:

1. **Top Products**: Most frequently mentioned medical products
2. **Price Analysis**: Product price variations across channels
3. **Visual Content**: Channels with most image content
4. **Trends**: Daily/weekly posting volume trends

## API Endpoints

- `GET /api/reports/top-products?limit=10` - Top mentioned products
- `GET /api/channels/{channel_name}/activity` - Channel activity
- `GET /api/search/messages?query=paracetamol` - Message search
- `GET /api/health` - Health check

## Development

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Setup database
python -m app.setup_database

# Run FastAPI
uvicorn app.main:app --reload

# Run Dagster
dagster dev
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

---

**Built by the Kara Solutions Data Engineering Team**
