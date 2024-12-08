# database.py
import pymysql
import logging
from config import Config

class DatabaseManager:
    def __init__(self):
        self.config = Config.DB_CONFIG
        self.logger = logging.getLogger(__name__)

    def get_connection(self):
        """Get database connection with explicit database selection"""
        # First connect without database specified
        conn = pymysql.connect(
            host=self.config['host'],
            user=self.config['user'],
            password=self.config['password'],
            connect_timeout=self.config['connect_timeout']
        )
        return conn

    def create_schema(self, connection):
        """Create database and table with explicit database creation and selection"""
        try:
            with connection.cursor() as cursor:
                # Create database if it doesn't exist
                cursor.execute("CREATE DATABASE IF NOT EXISTS wikistat_data")
                cursor.execute("USE wikistat_data")
                
                # Create table
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS pageviews (
                    `time` DATETIME,
                    `project` VARCHAR(50),
                    `subproject` VARCHAR(50),
                    `path` VARCHAR(2048),
                    `hits` BIGINT,
                    INDEX idx_time_path (`time`, `path`(255))
                ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
                """
                cursor.execute(create_table_sql)
                connection.commit()
                self.logger.info("Database and table created successfully")

                # Verify table exists
                cursor.execute("SHOW TABLES LIKE 'pageviews'")
                if not cursor.fetchone():
                    raise Exception("Table creation failed - table does not exist after creation")
                
        except Exception as e:
            self.logger.error(f"Error creating database/table: {e}")
            raise

    def insert_batch(self, connection, batch):
        """Insert batch with explicit database selection"""
        try:
            with connection.cursor() as cursor:
                # Ensure we're using the correct database
                cursor.execute("USE wikistat_data")
                
                insert_sql = """
                INSERT INTO pageviews (
                    `time`, `project`, `subproject`, `path`, `hits`
                ) VALUES (%s, %s, %s, %s, %s)
                """
                cursor.executemany(insert_sql, batch)
                connection.commit()
                return len(batch)
        except Exception as e:
            self.logger.error(f"Error inserting batch: {e}")
            connection.rollback()
            raise

    def verify_connection(self, connection):
        """Verify database and table exist and are accessible"""
        try:
            with connection.cursor() as cursor:
                # Check if we can use the database
                cursor.execute("USE wikistat_data")
                
                # Check if table exists
                cursor.execute("SHOW TABLES LIKE 'pageviews'")
                if not cursor.fetchone():
                    self.logger.error("Table 'pageviews' does not exist")
                    return False
                
                # Test select
                cursor.execute("SELECT 1 FROM pageviews LIMIT 1")
                return True
        except Exception as e:
            self.logger.error(f"Database verification failed: {e}")
            return False