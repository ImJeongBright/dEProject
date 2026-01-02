import os
import sys
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


load_dotenv()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PartitionSetup")

def get_db_engine():
    user = os.getenv('POSTGRES_USER', 'airflow')
    password = os.getenv('POSTGRES_PASSWORD', 'airflow')
    host = os.getenv('POSTGRES_HOST', 'postgres-warehouse')


    port = os.getenv('POSTGRES_PORT', '5432')
    db = os.getenv('POSTGRES_DB', 'bike_warehouse')

    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

def setup_partitions():
    engine = get_db_engine()

    logger.info("Starting Partition Migration...")


    try:
        with engine.begin() as conn:
            logger.info("--- 1. Bike Data: Renaming old table ---")
            conn.execute(text("ALTER TABLE IF EXISTS raw_data.bike_realtime RENAME TO bike_realtime_backup;"))

            logger.info("--- Bike Data: Creating Partitioned Table ---")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS raw_data.bike_realtime (
                    rack_cnt INTEGER,
                    station_name TEXT,
                    bike_cnt INTEGER,
                    shared INTEGER,
                    latitude FLOAT,
                    longitude FLOAT,
                    station_id TEXT,
                    execution_date TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) PARTITION BY RANGE (execution_date);
            """))

            logger.info("--- Bike Data: Creating Monthly Partitions (2024-2026) ---")
            for year in [2024, 2025, 2026]:
                for month in range(1, 13):
                    partition_name = f"bike_realtime_{year}_{month:02d}"
                    start_date = f"{year}-{month:02d}-01"

                    if month == 12:
                        end_date = f"{year+1}-01-01"
                    else:
                        end_date = f"{year}-{month+1:02d}-01"

                    conn.execute(text(f"""
                        CREATE TABLE IF NOT EXISTS raw_data.{partition_name}
                        PARTITION OF raw_data.bike_realtime
                        FOR VALUES FROM ('{start_date}') TO ('{end_date}');
                    """))

            logger.info("--- Bike Data: Migrating Data from Backup ---")


            conn.execute(text("""
                INSERT INTO raw_data.bike_realtime (rack_cnt, station_name, bike_cnt, shared, latitude, longitude, station_id, execution_date)
                SELECT
                    CAST(NULLIF(rack_cnt::text, '') AS INTEGER),
                    station_name,
                    CAST(NULLIF(bike_cnt::text, '') AS INTEGER),
                    CAST(NULLIF(shared::text, '') AS INTEGER),
                    CAST(NULLIF(latitude::text, '') AS FLOAT),
                    CAST(NULLIF(longitude::text, '') AS FLOAT),
                    station_id,
                    CAST(execution_date AS TIMESTAMP)
                FROM raw_data.bike_realtime_backup;
            """))
            logger.info("Data migration successful (Bike).")

    except Exception as e:
        logger.error(f"Bike Data Migration Failed (Rolled Back): {e}")


    logger.info("Partition setup completed.")

if __name__ == "__main__":
    setup_partitions()
