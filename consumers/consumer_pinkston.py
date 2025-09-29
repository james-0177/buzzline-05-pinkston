"""
consumer_pinkston.py

Consume json messages from a live data file. 
Insert the processed messages into a database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Database functions are in consumers/db_sqlite_case.py.
Environment variables are in utils/utils_config module. 

This file is based on file_consumer_case.py but modified by James Pinkston.
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import pathlib
import sys
import time
from datetime import datetime
from typing import Optional, Dict, Any
import sqlite3

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger
from .sqlite_consumer_case import init_db, insert_message

#####################################
# Function to process a single message
# #####################################

def process_message(raw_message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Process and transform a single JSON message into a processed message.
    Converts message fields to appropriate data types.

    """
    try:
        text = raw_message.get("message", "") or ""
        sentiment = float(raw_message.get("sentiment", 0.0))
        word_count = len(text.split())

        # sentiment labeling
        if sentiment > 0.6:
            label = "high"
        elif sentiment >= 0.4:
            label = "medium"
        else:
            label = "low"

        processed = {
            "message": text,
            "author": raw_message.get("author"),
            "timestamp": raw_message.get("timestamp"),
            "category": raw_message.get("category"),
            "sentiment": sentiment,
            "sentiment_label": label,
            "keyword_mentioned": raw_message.get("keyword_mentioned"),
            "message_length": int(raw_message.get("message_length", len(text))),
            "word_count": word_count,
            "processed_at": datetime.utcnow().isoformat(),
        }
        logger.info(f"Processed message: {processed}")
        return processed
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

#####################################
# Insights DB helpers
#####################################

def init_insights_db(db_path: pathlib.Path) -> None:
    """
    Initialize the insights_pinkston table if it doesn't exist.
    """
    str_path = str(db_path)
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS insights_pinkston (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    author TEXT,
                    timestamp TEXT,
                    category TEXT,
                    sentiment REAL,
                    sentiment_label TEXT,
                    keyword_mentioned TEXT,
                    message_length INTEGER,
                    word_count INTEGER,
                    processed_at TEXT
                );
                """
            )
            conn.commit()
        logger.info(f"Initialized insights DB/table at {str_path}.")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize insights DB: {e}")
        raise

def insert_insight(insight: Dict[str, Any], db_path: pathlib.Path) -> None:
    """
    Insert a single processed insight row into the insights_pinkston table.
    """
    str_path = str(db_path)
    try:
        with sqlite3.connect(str_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO insights_pinkston (
                    message, author, timestamp, category, sentiment, sentiment_label,
                    keyword_mentioned, message_length, word_count, processed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    insight.get("message"),
                    insight.get("author"),
                    insight.get("timestamp"),
                    insight.get("category"),
                    insight.get("sentiment"),
                    insight.get("sentiment_label"),
                    insight.get("keyword_mentioned"),
                    insight.get("message_length"),
                    insight.get("word_count"),
                    insight.get("processed_at"),
                ),
            )
            conn.commit()
        logger.info("Inserted one insight row into insights_pinkston.")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert insight into DB: {e}")

#####################################
# Consume Messages from Live Data File
#####################################


def consume_messages_from_file(live_data_path, sql_path, interval_secs, last_position):
    """
    Consume new messages from a file and process them.
    Each message is expected to be JSON-formatted.

    Args:
    - live_data_path (pathlib.Path): Path to the live data file.
    - sql_path (pathlib.Path): Path to the SQLite database file.
    - interval_secs (int): Interval in seconds to check for new messages.
    - last_position (int): Last read position in the file.
    """
    logger.info("Called consume_messages_from_file() with:")
    logger.info(f"   {live_data_path=}")
    logger.info(f"   {sql_path=}")
    logger.info(f"   {interval_secs=}")
    logger.info(f"   {last_position=}")
    
    logger.info("1. Initialize the database.")
    init_db(sql_path)
    init_insights_db(sql_path)

    logger.info("2. Set the last position to 0 to start at the beginning of the file.")
    last_position = 0

    while True:
        try:
            logger.info(f"3. Read from live data file at position {last_position}.")
            with open(live_data_path, "r") as file:
                # Move to the last read position
                file.seek(last_position)
                for line in file:
                    # If we strip whitespace and there is content
                    if not line.strip():
                        continue

                    # Use json.loads to parse the stripped line
                    raw = json.loads(line.strip())

                    # Call our process_message function
                    processed = process_message(raw)
                    if processed:
                        insert_message(processed, sql_path)
                        insert_insight(processed, sql_path)

                    # Update the last position that's been read to the current file position
                    last_position = file.tell()

            # sleep before checking for more lines
            time.sleep(interval_secs) 

        except FileNotFoundError:
            logger.error(f"ERROR: Live data file not found at {live_data_path}. Make sure the producer is running.")
            time.sleep(interval_secs)
        except Exception as e:
            logger.error(f"ERROR: Error reading from live data file: {e}")
            sys.exit(11)

#####################################
# Define Main Function
#####################################


def main():
    """
    Main function to run the consumer process.

    Reads configuration, initializes the database, and starts consumption.

    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path: pathlib.Path = config.get_live_data_path()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete any prior database file for a fresh start.")
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    logger.info("STEP 3. Initialize a new database with an empty table.")
    try:
        init_db(sqlite_path)
        init_insights_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_file(live_data_path, sqlite_path, interval_secs, 0)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        logger.info("TRY/FINALLY: Consumer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
