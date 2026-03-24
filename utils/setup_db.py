

# utils/setup_db.py
import sys
import logging
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.db_wrapper import DBWrapper

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_database():
    """Create all necessary tables and indexes for the pipeline"""
    db = DBWrapper()
    
    logger.info("Creating tables...")
    
    try:
        with db.conn.cursor() as cur:
            queries = [
                # FINANCIALS (APPEND ONLY - no unique constraint)
                """
                CREATE TABLE IF NOT EXISTS financials (
                    company_number TEXT,
                    metric TEXT,
                    value NUMERIC,
                    period TEXT,
                    account_closing_date_last_year DATE,
                    fiscal_period_new INT,
                    is_consolidated TEXT,
                    data_scope TEXT,
                    filing_date DATE,
                    source_file TEXT,
                    ch_upload TEXT,
                    loaded_at TIMESTAMP DEFAULT NOW()
                );
                """,

                # DIRECTORS (UPSERT - unique per company and director)
                """
                CREATE TABLE IF NOT EXISTS directors (
                    company_number TEXT,
                    director_name TEXT,
                    filing_date DATE,
                    source_file TEXT,
                    ch_upload TEXT,
                    loaded_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(company_number, director_name)
                );
                """,
                """
                CREATE TABLE IF NOT EXISTS processed_files (
                    source_file TEXT PRIMARY KEY,
                    ch_upload TEXT,
                    processed_at TIMESTAMP DEFAULT NOW()
                );
               """
               ,

                # REPORTS (UPSERT - unique per company and section)
                """
                CREATE TABLE IF NOT EXISTS reports (
                    company_number TEXT,
                    company_name TEXT,
                    section TEXT,
                    text TEXT,
                    filing_date DATE,
                    source_file TEXT,
                    ch_upload TEXT,
                    loaded_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(company_number, section)
                );
                """,

                # METADATA (UPSERT - unique per company and field)
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    company_number TEXT,
                    field TEXT,
                    value TEXT,
                    filing_date DATE,
                    source_file TEXT,
                    ch_upload TEXT,
                    loaded_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(company_number, field)
                );
                """,

                # PIPELINE TRACKING
                """
                CREATE TABLE IF NOT EXISTS ch_pipeline_runs (
                    ch_upload TEXT PRIMARY KEY,
                    zip_filename TEXT,
                    files_processed INT,
                    files_failed INT,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    status TEXT
                );
                """
            ]

            # Execute table creation queries
            for q in queries:
                cur.execute(q)
            
            # Create indexes
            logger.info("Creating indexes...")
            
            index_queries = [
                "CREATE INDEX IF NOT EXISTS idx_financials_company ON financials(company_number);",
                "CREATE INDEX IF NOT EXISTS idx_directors_company ON directors(company_number);",
                "CREATE INDEX IF NOT EXISTS idx_reports_company ON reports(company_number);",
                "CREATE INDEX IF NOT EXISTS idx_metadata_company ON metadata(company_number);",

            ]
            
            for idx_query in index_queries:
                cur.execute(idx_query)

        logger.info("Tables and indexes created successfully!")

        return True

    except Exception as e:
        logger.error(f"Database setup error: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()
