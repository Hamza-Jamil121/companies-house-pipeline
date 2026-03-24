
# run_pipeline.py
import os
import sys
import glob
import csv
import tempfile
import logging
from pathlib import Path
from multiprocessing import Pool
from datetime import datetime
import time

# Add parent directory to path so we can import modules
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))

from test_pipeline import parse_one_file
from config import *
from utils.db_wrapper import DBWrapper
from utils.pipeline_utils import download_zip, extract_zip
from utils.setup_db import setup_database
# from utils.email_alert import notify
# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

FAILED_LOG = "failed_files.log"

# -----------------------------
# DATABASE CHECK
# -----------------------------
def check_database_ready():
    """Check if database tables exist"""
    try:
        db = DBWrapper()
        with db.conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'financials'
                )
            """)
            exists = cur.fetchone()[0]
        db.close()
        return exists
    except Exception as e:
        logger.warning(f"Database connection error: {e}")
        return False

# -----------------------------
# PROCESS FILE
# -----------------------------
def process_file(filepath):
    """Parse a single file safely"""
    try:
        result = parse_one_file(filepath)
        return (filepath, result, None)  # Include original filepath
    except Exception as e:
        return (filepath, None, str(e))

# -----------------------------
# FILTER NEW FILES
# -----------------------------
def filter_new_files(db, files):
    with db.conn.cursor() as cur:
        cur.execute("SELECT source_file FROM processed_files")
        processed = set(row[0] for row in cur.fetchall())

    new_files = [
        f for f in files
        if os.path.splitext(os.path.basename(f))[0] not in processed
    ]
    logger.info(f"Filtered new files: {len(new_files)} / {len(files)}")
    return new_files

# -----------------------------
# PROCESS BATCH
# -----------------------------
def process_batch(file_results):
    """Process a batch of parsed file results and write to database"""
    db = DBWrapper()
    financials, directors, reports, metadata = [], [], [], []
    failed = []
    source_files = set()

    for filepath, result, error in file_results:
        if error or not result:
            failed.append((filepath, error or "Empty result"))
            continue

        # Track source files
        for section in ["financials", "directors", "text_sections", "metadata"]:
            if section in result:
                for row in result[section]:
                    if "source_file" in row:
                        source_files.add(row["source_file"])

        # Append data
        for f in result.get("financials", []):
            financials.append((
                f["company_number"], f["metric"], f["value"], f["period"],
                f["account_closing_date_last_year"], f["fiscal_period_new"],
                f["is_consolidated"], f["data_scope"], f["filing_date"],
                f["source_file"], f["ch_upload"]
            ))
        for d in result.get("directors", []):
            directors.append((
                d["company_number"], d["director_name"],
                d["filing_date"], d["source_file"], d["ch_upload"]
            ))
        for r in result.get("text_sections", []):
            reports.append((
                r["company_number"], r["company_name"],
                r["section"], r["text"], r["filing_date"],
                r["source_file"], r["ch_upload"]
            ))
        for m in result.get("metadata", []):
            metadata.append((
                m["company_number"], m["field"], m["value"],
                m["filing_date"], m["source_file"], m["ch_upload"]
            ))

    # Deduplicate
    directors = list({(d[0], d[1]): d for d in directors}.values())
    reports = list({(r[0], r[2]): r for r in reports}.values())
    metadata = list({(m[0], m[1]): m for m in metadata}.values())

    tmpfile_path = None
    try:
        # Write financials CSV
        if financials:
            with tempfile.NamedTemporaryFile(mode='w+', newline='', delete=False, suffix='.csv') as tmpfile:
                writer = csv.writer(tmpfile)
                writer.writerows(financials)
                tmpfile_path = tmpfile.name
            db.copy_financials_from_csv(tmpfile_path)

        if directors:
            db.upsert_directors(directors)
        if reports:
            db.upsert_reports(reports)
        if metadata:
            db.upsert_metadata(metadata)

        if source_files:
            db.insert_processed_files(list(source_files), PIPELINE_MONTH)

    except Exception as e:
        logger.error(f"Database write error: {e}")
        raise
    finally:
        db.close()
        if tmpfile_path and os.path.exists(tmpfile_path):
            os.remove(tmpfile_path)

    # Log failed files
    if failed:
        with open(FAILED_LOG, 'a', encoding='utf-8') as f:
            f.write(f"\n{'='*60}\n")
            f.write(f"RUN: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"{'='*60}\n")
            for filename, error in failed:
                f.write(f"{filename} -> {error}\n")
            f.write(f"Total failed in batch: {len(failed)}\n")
        logger.warning(f"{len(failed)} files failed to parse - see {FAILED_LOG}")

    return [f[0] for f in failed]

# -----------------------------
# RETRY FAILED FILES
# -----------------------------
def retry_failed_files(failed_files, retries=3, delay=5):
    logger.info(f"Retrying {len(failed_files)} failed files, max {retries} attempts...")
    for attempt in range(1, retries + 1):
        logger.info(f"Retry attempt {attempt}/{retries}")
        new_failed = []
        # ffiles = filter_new_files(db, files)
        for filepath in failed_files:
            fp, result, error = process_file(filepath)
            if error or not result:
                new_failed.append(filepath)
                logger.warning(f"Retry failed for {filepath}: {error}")
            else:
                process_batch([(fp, result, None)])
                logger.info(f"Retry succeeded for {filepath}")
        if not new_failed:
            logger.info("All failed files processed successfully on retry")
            return []
        failed_files = new_failed
        if failed_files and attempt < retries:
            logger.info(f"Waiting {delay} seconds before next retry...")
            time.sleep(delay)
    return failed_files

# -----------------------------
# CLEANUP
# -----------------------------
def cleanup_files(directory):
    try:
        for root, _, files in os.walk(directory):
            for f in files:
                os.remove(os.path.join(root, f))
        logger.info(f"Cleaned up files in {directory}")
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

# -----------------------------
# VERIFICATION
# -----------------------------
def run_verification():
    try:
        from utils.verification import run_verification as verify
        return verify()
    except ImportError:
        logger.warning("verification.py not found, skipping verification")
        return True

# -----------------------------
# MAIN
# -----------------------------
def main():
    start_time = datetime.now()
    run_id = start_time.strftime("%Y%m%d_%H%M%S")

    logger.info("="*80)
    logger.info(f"PIPELINE RUN STARTED - {run_id}")
    logger.info(f"Month: {PIPELINE_MONTH}")
    logger.info("="*80)

    # Step 1: Database setup
    logger.info("Step 1: Checking database setup...")
    if not check_database_ready():
        setup_database()
        logger.info("Database setup completed!")

    # Step 2: Check previous pipeline
    db = DBWrapper()
    already_processed = False
    try:
        with db.conn.cursor() as cur:
            cur.execute("SELECT status FROM ch_pipeline_runs WHERE ch_upload=%s", (PIPELINE_MONTH,))
            result = cur.fetchone()
            if result and result[0] == 'completed':
                logger.info(f"Pipeline already completed for {PIPELINE_MONTH}. Skipping processing.")
                already_processed = True
    finally:
        db.close()
    if already_processed:
        run_verification()
        logger.info(f"PIPELINE RUN COMPLETED (SKIPPED) - {run_id}")
        # notify(
        #         status="success",
        #         month=PIPELINE_MONTH,
        #         processed=0,
        #         failed=0,
        #         duration="Already processed (skipped)"
        #     )   for example purpose email and alert
        return

    # Step 3: Download and extract
    logger.info("Step 3: Downloading and extracting data...")
    download_zip(DOWNLOAD_URL, ZIP_PATH)
    extract_zip(ZIP_PATH, EXTRACT_DIR)

    # Step 4: Collect files
    logger.info("Step 4: Collecting HTML files...")
    files = glob.glob(f"{EXTRACT_DIR}/**/*.html", recursive=True)
    logger.info(f"Total files found: {len(files)}")
    if not files:
        logger.warning("No HTML files found to process")
        cleanup_files(EXTRACT_DIR)
        return

    # Step 5: Filter already processed
    db = DBWrapper()
    try:
        files = filter_new_files(db, files)
        db.insert_pipeline_run((
            PIPELINE_MONTH, Path(ZIP_PATH).name, 0, 0, datetime.now(), None, "running"
        ))
    finally:
        db.close()
    if not files:
        logger.info("No new files to process")
        cleanup_files(EXTRACT_DIR)
        return

    # Step 6: Process files in parallel
    logger.info("Step 6: Processing files in parallel...")
    results_buffer = []
    total_failed_files = []
    processed_count = 0

    with Pool(NUM_WORKERS) as pool:
        for result in pool.imap_unordered(process_file, files, chunksize=50):
            results_buffer.append(result)
            processed_count += 1
            if processed_count % BATCH_SIZE == 0:
                logger.info(f"Processed {processed_count}/{len(files)} files")
                failed_files = process_batch(results_buffer)
                total_failed_files.extend(failed_files)
                results_buffer = []

        # Process remaining
        if results_buffer:
            failed_files = process_batch(results_buffer)
            total_failed_files.extend(failed_files)

    logger.info(f"Processing complete: {len(files) - len(total_failed_files)} succeeded, {len(total_failed_files)} failed")

    # Retry failed files
    if total_failed_files:
        remaining_failed_files = retry_failed_files(total_failed_files)
        total_failed_files = remaining_failed_files

    # Step 7: Update pipeline status

    # Step 7: Update pipeline status with cumulative count
    db = DBWrapper()
    try:

        with db.conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM processed_files")
            total_processed = cur.fetchone()[0]

            cur.execute("""
                UPDATE ch_pipeline_runs
                SET files_processed=%s,
                    files_failed=%s,
                    completed_at=%s,
                    status='completed'
                WHERE ch_upload=%s
            """, (total_processed, len(total_failed_files), datetime.now(), PIPELINE_MONTH))


    finally:
        db.close()

    logger.info(f"Updated ch_pipeline_runs: Total files processed (cumulative) = {total_processed}, Failed = {len(total_failed_files)}")


    # Step 8: Cleanup
    cleanup_files(EXTRACT_DIR)

    # Step 9: Final summary
    duration = datetime.now() - start_time
    success_rate = (total_processed / (total_processed + len(total_failed_files))) * 100

    logger.info("="*60)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY")
    logger.info(f"   Run ID: {run_id}")
    logger.info(f"   Month: {PIPELINE_MONTH}")
    logger.info(f"   Files processed: {len(files) - len(total_failed_files)}")
    logger.info(f"   Files failed: {len(total_failed_files)}")
    logger.info(f"   Success rate: {success_rate:.1f}%")
    logger.info(f"   Duration: {duration}")
    logger.info(f"   Log file: pipeline.log")
    logger.info(f"   Failed files log: {FAILED_LOG}")
    logger.info("="*60)

    # Step 10: Verification
    logger.info("Step 10: Running data verification...")
    if run_verification():
        logger.info("✅ Verification passed! All data looks good.")
    else:
        logger.warning("⚠️ Verification found issues - check logs above")

    logger.info("="*80)
    logger.info(f"PIPELINE RUN COMPLETED - {run_id}")
    logger.info("="*80)

if __name__ == "__main__":
    main()











