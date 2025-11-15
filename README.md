# Connecticut Real Estate Sales Pipeline → Snowflake

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Ready-29B5E8.svg)](https://www.snowflake.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A complete ETL pipeline that ingests Connecticut real estate sales data (2001-2023) into Snowflake using the Medallion Architecture (Bronze → Silver → Gold).

## 📊 Project Overview

- **Data Source:** Connecticut Open Data Portal (1M+ sales records)
- **Architecture:** Medallion (Bronze/Silver/Gold layers)
- **Tech Stack:** Python, Snowflake, Pandas
- **Analytics:** Town statistics, property trends, market insights

## 🏗️ Architecture

Connecticut Open Data API
↓
[Python Script]
↓
BRONZE (Raw Data)
↓
SILVER (Validated)
↓
GOLD (Analytics)
↓
Dashboards & Reports

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Snowflake account
- Git (optional)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/ct-realestate-snowflake.git
cd ct-realestate-snowflake
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure environment**
Create `.env` file:
```env
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=CT_REAL_ESTATE
SNOWFLAKE_ROLE=ACCOUNTADMIN
CT_DATA_URL=https://data.ct.gov/api/views/5mzw-sjtu/rows.csv?accessType=DOWNLOAD
```

5. **Setup Snowflake database**
Run the SQL scripts in Snowflake:
```bash
# See sql/01_setup_database.sql
```

6. **Run the pipeline**
```bash
# Test with 10K records
python ingest_data.py
# Select option 1 for test mode

# Full load (1M+ records)
python ingest_data.py
# Select option 2 for full load
```

## 📁 Project Structure

ct-realestate-snowflake/
├── .env.example          # Environment template
├── .gitignore           # Git ignore rules
├── README.md            # This file
├── requirements.txt     # Python dependencies
├── test_connection.py   # Snowflake connection test
├── ingest_data.py      # Main ingestion script
├── sql/
│   ├── 01_setup_database.sql
│   ├── 02_transform_silver.sql
│   ├── 03_aggregate_gold.sql
│   └── 04_analytics_queries.sql
└── docs/
└── architecture.md

## 📊 Data Layers

### Bronze Layer (Raw)
- Raw CSV data as-is from Connecticut Open Data
- No transformations
- Complete audit trail with load timestamps

### Silver Layer (Validated)
- Data type conversions
- Quality checks (valid prices, dates, locations)
- Calculated fields (year, month, quarter)
- Quality scoring (0-100)

### Gold Layer (Analytics)
- Town-level yearly statistics
- Property type trends
- Market indicators
- Price indices

## 📈 Sample Analytics

### Top Towns by Average Price
```sql
SELECT 
    town,
    year,
    total_sales,
    avg_sale_price,
    median_sale_price
FROM CT_REAL_ESTATE.GOLD.town_yearly_stats
WHERE year = 2023
ORDER BY avg_sale_price DESC
LIMIT 10;
```

### Property Type Distribution
```sql
SELECT
    property_type,
    COUNT(*) as total_sales,
    AVG(sale_amount) as avg_price
FROM CT_REAL_ESTATE.SILVER.sales_validated
WHERE is_valid_sale = TRUE
GROUP BY property_type;
```

## 🔄 Automation

The pipeline supports scheduled runs using Snowflake Tasks:
```sql
-- Daily refresh
CREATE TASK daily_refresh
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 2 * * * America/New_York'
AS
  CALL refresh_pipeline();
```

## 📝 Data Quality

The pipeline includes comprehensive data quality checks:
- ✅ Valid sale amounts (≥ $2,000)
- ✅ Valid dates and town names
- ✅ Arms-length transactions only
- ✅ Quality scoring (0-100)

## 🛠️ Tech Stack

- **Python 3.11+** - ETL scripting
- **Snowflake** - Data warehouse
- **Pandas** - Data manipulation
- **Requests** - API calls
- **python-dotenv** - Configuration management

## 📊 Key Metrics

- **Total Records:** 1,000,000+
- **Time Period:** 2001-2023
- **Towns Covered:** 169 Connecticut municipalities
- **Update Frequency:** Annual (Oct 1 - Sep 30)

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Connecticut Open Data Portal for providing the dataset
- Snowflake for the data platform

## 📧 Contact

Dokleat Halilaj

## 📚 Related Articles

- [Medium Article: Building a Real Estate Analytics Pipeline with Snowflake](#)
- [Documentation: Medallion Architecture Guide](docs/architecture.md)
