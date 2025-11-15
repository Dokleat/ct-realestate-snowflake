"""
Connecticut Real Estate Data Ingestion
Downloads data from CT Open Data Portal and loads to Snowflake

Run: python ingest_data.py
"""

import snowflake.connector
import requests
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

class CTRealEstateIngestion:
    """Ingest Connecticut Real Estate data to Snowflake"""
    
    def __init__(self):
        """Initialize connection to Snowflake"""
        logger.info("Connecting to Snowflake...")
        
        self.conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            role=os.getenv('SNOWFLAKE_ROLE')
        )
        self.cursor = self.conn.cursor()
        self.data_url = os.getenv('CT_DATA_URL')
        
        logger.info("✓ Connected to Snowflake")
        
    def download_data(self, limit=None):
        """Download CSV data from Connecticut Open Data Portal"""
        logger.info("=" * 60)
        logger.info("STEP 1: Downloading data from Connecticut Open Data...")
        logger.info(f"URL: {self.data_url}")
        
        try:
            # Download CSV with timeout
            response = requests.get(self.data_url, timeout=300)
            response.raise_for_status()
            
            # Parse CSV
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            
            # Limit rows if specified
            if limit:
                logger.info(f"Limiting to {limit} records for testing")
                df = df.head(limit)
            
            logger.info(f"✓ Downloaded {len(df):,} records")
            logger.info(f"  Columns: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"✗ Error downloading data: {e}")
            raise
    
    def clean_data(self, df):
        """Basic data cleaning"""
        logger.info("=" * 60)
        logger.info("STEP 2: Cleaning data...")
        
        # Replace NaN with None
        df = df.where(pd.notnull(df), None)
        
        # Strip whitespace from string columns
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        
        logger.info(f"✓ Data cleaned: {len(df):,} records ready")
        
        return df
    
    def load_to_bronze(self, df, batch_size=5000):
        """Load data to Bronze layer in batches"""
        logger.info("=" * 60)
        logger.info("STEP 3: Loading data to Snowflake BRONZE layer...")
        
        # Truncate existing data
        logger.info("  Clearing existing data...")
        self.cursor.execute("TRUNCATE TABLE CT_REAL_ESTATE.BRONZE.raw_sales")
        
        # Insert in batches
        total_rows = len(df)
        inserted_count = 0
        
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch = df.iloc[start_idx:end_idx]
            
            # Prepare insert values
            values = []
            for _, row in batch.iterrows():
                value_tuple = (
                    str(row.get('Serial Number', '') or ''),
                    str(row.get('List Year', '') or ''),
                    str(row.get('Date Recorded', '') or ''),
                    str(row.get('Town', '') or ''),
                    str(row.get('Address', '') or ''),
                    str(row.get('Assessed Value', '') or ''),
                    str(row.get('Sale Amount', '') or ''),
                    str(row.get('Sales Ratio', '') or ''),
                    str(row.get('Property Type', '') or ''),
                    str(row.get('Residential Type', '') or ''),
                    str(row.get('Non Use Code', '') or ''),
                    str(row.get('Assessor Remarks', '') or ''),
                    str(row.get('OPM remarks', '') or ''),
                    str(row.get('Location', '') or '')
                )
                values.append(value_tuple)
            
            # Insert batch
            insert_sql = """
            INSERT INTO CT_REAL_ESTATE.BRONZE.raw_sales 
            (serial_number, list_year, date_recorded, town, address, 
             assessed_value, sale_amount, sales_ratio, property_type, 
             residential_type, non_use_code, assessor_remarks, opm_remarks, location)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            self.cursor.executemany(insert_sql, values)
            inserted_count += len(values)
            
            # Progress update
            progress = (end_idx / total_rows) * 100
            logger.info(f"  Progress: {progress:.1f}% ({end_idx:,}/{total_rows:,} records)")
        
        self.conn.commit()
        logger.info(f"✓ Successfully loaded {inserted_count:,} records to BRONZE")
        
    def verify_load(self):
        """Verify data was loaded correctly"""
        logger.info("=" * 60)
        logger.info("STEP 4: Verifying data load...")
        
        # Count records
        self.cursor.execute("SELECT COUNT(*) FROM CT_REAL_ESTATE.BRONZE.raw_sales")
        count = self.cursor.fetchone()[0]
        
        # Sample data
        self.cursor.execute("""
            SELECT town, COUNT(*) as count 
            FROM CT_REAL_ESTATE.BRONZE.raw_sales 
            GROUP BY town 
            ORDER BY count DESC 
            LIMIT 5
        """)
        top_towns = self.cursor.fetchall()
        
        logger.info(f"✓ Total records in BRONZE: {count:,}")
        logger.info(f"  Top 5 towns by sales:")
        for town, town_count in top_towns:
            logger.info(f"    - {town}: {town_count:,} sales")
    
    def run_pipeline(self, limit=None):
        """Execute complete ingestion pipeline"""
        try:
            start_time = datetime.now()
            logger.info("=" * 60)
            logger.info("CONNECTICUT REAL ESTATE DATA PIPELINE")
            logger.info("=" * 60)
            
            # Step 1: Download
            df = self.download_data(limit=limit)
            
            # Step 2: Clean
            df = self.clean_data(df)
            
            # Step 3: Load
            self.load_to_bronze(df)
            
            # Step 4: Verify
            self.verify_load()
            
            # Summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("=" * 60)
            logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info(f"  Duration: {duration:.1f} seconds")
            logger.info(f"  Records processed: {len(df):,}")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error("=" * 60)
            logger.error("✗ PIPELINE FAILED!")
            logger.error(f"  Error: {e}")
            logger.error("=" * 60)
            raise
        finally:
            self.cursor.close()
            self.conn.close()

if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("CT REAL ESTATE INGESTION PIPELINE")
    print("=" * 60)
    print("\nOptions:")
    print("  1. Test run (10,000 records)")
    print("  2. Full load (all data)")
    
    choice = input("\nEnter choice (1 or 2): ").strip()
    
    if choice == "1":
        print("\n→ Running TEST MODE with 10,000 records...\n")
        ingestion = CTRealEstateIngestion()
        ingestion.run_pipeline(limit=10000)
    elif choice == "2":
        confirm = input("\nFull load will download 1M+ records. Continue? (yes/no): ").strip().lower()
        if confirm == 'yes':
            print("\n→ Running FULL LOAD...\n")
            ingestion = CTRealEstateIngestion()
            ingestion.run_pipeline()
        else:
            print("Cancelled.")
    else:
        print("Invalid choice. Please run again and enter 1 or 2.")