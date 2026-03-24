
 
import psycopg2,os,sys
from psycopg2.extras import execute_values
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))
from config import DB_CONFIG

class DBWrapper:

    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = True

    def close(self):
        self.conn.close()

    # ─────────────────────────────────────────
    # GENERIC BATCH INSERT
    # ─────────────────────────────────────────
    def insert_batch(self, table, columns, rows, conflict_clause=None):
        if not rows:
            return

        query = f"""
            INSERT INTO {table} ({','.join(columns)})
            VALUES %s
        """

        if conflict_clause:
            query += f" {conflict_clause}"

        with self.conn.cursor() as cur:
            execute_values(cur, query, rows)

    # ─────────────────────────────────────────
    # FINANCIALS → APPEND ONLY
    # ─────────────────────────────────────────
    def insert_financials(self, rows):
        cols = [
            "company_number", "metric", "value", "period",
            "account_closing_date_last_year", "fiscal_period_new",
            "is_consolidated", "data_scope", "filing_date",
            "source_file", "ch_upload"
        ]

        self.insert_batch("financials", cols, rows)

    # ─────────────────────────────────────────
    # UPSERT TABLES
    # ─────────────────────────────────────────
    def upsert_directors(self, rows):
        cols = ["company_number", "director_name", "filing_date", "source_file", "ch_upload"]

        conflict = """
        ON CONFLICT (company_number, director_name)
        DO UPDATE SET
            filing_date = EXCLUDED.filing_date,
            source_file = EXCLUDED.source_file,
            ch_upload = EXCLUDED.ch_upload
        """

        self.insert_batch("directors", cols, rows, conflict)

    def upsert_reports(self, rows):
        cols = ["company_number", "company_name", "section", "text",
                "filing_date", "source_file", "ch_upload"]

        conflict = """
        ON CONFLICT (company_number, section)
        DO UPDATE SET
            company_name = EXCLUDED.company_name,
            text = EXCLUDED.text,
            filing_date = EXCLUDED.filing_date,
            source_file = EXCLUDED.source_file,
            ch_upload = EXCLUDED.ch_upload
        """

        self.insert_batch("reports", cols, rows, conflict)

    def upsert_metadata(self, rows):
        cols = ["company_number", "field", "value",
                "filing_date", "source_file", "ch_upload"]

        conflict = """
        ON CONFLICT (company_number, field)
        DO UPDATE SET
            value = EXCLUDED.value,
            filing_date = EXCLUDED.filing_date,
            source_file = EXCLUDED.source_file,
            ch_upload = EXCLUDED.ch_upload
        """

        self.insert_batch("metadata", cols, rows, conflict)

    # ─────────────────────────────────────────
    # PIPELINE TRACKING
    # ─────────────────────────────────────────
    def insert_pipeline_run(self, row):
        query = """
        INSERT INTO ch_pipeline_runs (
            ch_upload, zip_filename, files_processed,
            files_failed, started_at, completed_at, status
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (ch_upload) DO NOTHING
        """

        with self.conn.cursor() as cur:
            cur.execute(query, row)

    def copy_financials_from_csv(self, csv_path):
        with self.conn.cursor() as cur:
            cur.copy_expert(
                """
                COPY financials (
                    company_number, metric, value, period,
                    account_closing_date_last_year, fiscal_period_new,
                    is_consolidated, data_scope, filing_date,
                    source_file, ch_upload
                ) FROM STDIN WITH CSV
                """,
                open(csv_path, 'r')
            )
    from psycopg2.extras import execute_values

    def insert_processed_files(self, source_files, ch_upload):
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO processed_files (source_file, ch_upload)
                VALUES %s
                ON CONFLICT (source_file) DO NOTHING
                """,
                [(sf, ch_upload) for sf in source_files]
            )
        self.conn.commit()
    def get_total_processed_files(self, month):

        """
        Returns the cumulative number of successfully processed files for a given month.
        """
        query = f"""
            SELECT SUM(files_processed) 
            FROM ch_pipeline_runs
            WHERE ch_upload = '{month}'
        """
        result = self.execute(query)
        return result[0][0] if result[0][0] is not None else 0



