# processing/data_processor.py - Process and clean data
import logging
import duckdb
import json
import pandas as pd
import os
from datetime import datetime
from typing import List, Dict, Any, Optional
from utils.helpers import time_function, validate_event, safe_json_loads

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self, db_path: str = "data/duckdb/github_events.db"):
        self.db_path = db_path
        self.conn = None
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    @time_function
    def connect(self):
        """Connect to DuckDB database"""
        try:
            self.conn = duckdb.connect(self.db_path)
            logger.info(f"Connected to DuckDB: {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to DuckDB: {e}")
            return False
    
    @time_function
    def setup_database(self):
        """Setup database tables and schema"""
        if not self.conn:
            logger.error("Not connected to database")
            return False
        
        try:
            # Create main events table
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS github_events (
                    event_id STRING PRIMARY KEY,
                    event_type STRING,
                    repo_name STRING,
                    repo_owner STRING,
                    actor_login STRING,
                    org_login STRING,
                    created_at TIMESTAMP,
                    is_public BOOLEAN,
                    payload JSON,
                    raw_event JSON,
                    processed_at TIMESTAMP,
                    
                    -- Derived columns
                    hour_of_day INTEGER,
                    day_of_week STRING,
                    month STRING,
                    year INTEGER
                )
            """)
            
            # Create indexes for common queries
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_event_type ON github_events(event_type)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_repo_name ON github_events(repo_name)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON github_events(created_at)")
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_actor ON github_events(actor_login)")
            
            # Create aggregated views for faster queries
            self.conn.execute("""
                CREATE OR REPLACE VIEW daily_aggregates AS
                SELECT 
                    DATE(created_at) as date,
                    event_type,
                    COUNT(*) as event_count,
                    COUNT(DISTINCT repo_name) as unique_repos,
                    COUNT(DISTINCT actor_login) as unique_actors
                FROM github_events
                GROUP BY DATE(created_at), event_type
                ORDER BY date DESC, event_count DESC
            """)
            
            logger.info("Database schema created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up database: {e}")
            return False
    
    @time_function
    def process_events(self, events: List[Dict[str, Any]]) -> pd.DataFrame:
        """Process and clean events"""
        logger.info(f"Processing {len(events):,} events...")
        
        processed_events = []
        skipped_events = 0
        
        for event in events:
            try:
                # Validate event
                if not validate_event(event):
                    skipped_events += 1
                    continue
                
                # Extract and transform data
                processed = self._transform_event(event)
                if processed:
                    processed_events.append(processed)
                
            except Exception as e:
                logger.warning(f"Error processing event {event.get('id')}: {e}")
                skipped_events += 1
                continue
        
        # Convert to DataFrame
        if processed_events:
            df = pd.DataFrame(processed_events)
            logger.info(f"Processed {len(df):,} events (skipped {skipped_events})")
            return df
        else:
            logger.warning("No events processed successfully")
            return pd.DataFrame()
    
    def _transform_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform a single event"""
        try:
            # Extract basic fields
            event_id = str(event.get('id', ''))
            event_type = event.get('type', '')
            repo_name = event.get('repo', {}).get('name', '')
            
            # Parse timestamp
            created_at_str = event.get('created_at', '')
            created_at = pd.to_datetime(created_at_str, errors='coerce')
            if pd.isna(created_at):
                return None
            
            # Extract repo owner
            repo_owner = ''
            if '/' in repo_name:
                repo_owner = repo_name.split('/')[0]
            
            # Create processed event
            processed = {
                'event_id': event_id,
                'event_type': event_type,
                'repo_name': repo_name,
                'repo_owner': repo_owner,
                'actor_login': event.get('actor', {}).get('login', ''),
                'org_login': event.get('org', {}).get('login', ''),
                'created_at': created_at,
                'is_public': event.get('public', True),
                'payload': json.dumps(event.get('payload', {})),
                'raw_event': json.dumps(event),
                'processed_at': datetime.now(),
                
                # Derived columns
                'hour_of_day': created_at.hour,
                'day_of_week': created_at.strftime('%A'),
                'month': created_at.strftime('%Y-%m'),
                'year': created_at.year
            }
            
            return processed
            
        except Exception as e:
            logger.warning(f"Error transforming event: {e}")
            return None
    
    @time_function
    def load_to_duckdb(self, df: pd.DataFrame, batch_size: int = 10000):
        """Load processed data to DuckDB"""
        if not self.conn:
            logger.error("Not connected to database")
            return False
        
        if df.empty:
            logger.warning("No data to load")
            return False
        
        try:
            total_rows = len(df)
            logger.info(f"Loading {total_rows:,} rows to DuckDB...")
            
            # Load in batches to manage memory
            for i in range(0, total_rows, batch_size):
                batch = df.iloc[i:i + batch_size]
                
                # Register batch as a temporary table
                self.conn.register('temp_events', batch)
                
                # Insert or replace data
                self.conn.execute("""
                    INSERT OR REPLACE INTO github_events 
                    SELECT * FROM temp_events
                """)
                
                # Unregister temporary table
                self.conn.unregister('temp_events')
                
                if (i + len(batch)) % 50000 == 0:
                    logger.info(f"Loaded {i + len(batch):,} rows...")
            
            logger.info(f"Successfully loaded {total_rows:,} rows to DuckDB")
            return True
            
        except Exception as e:
            logger.error(f"Error loading to DuckDB: {e}")
            return False
    
    @time_function
    def run_quality_checks(self) -> Dict[str, Any]:
        """Run data quality checks"""
        if not self.conn:
            return {}
        
        try:
            checks = {}
            
            # Check 1: Total events
            result = self.conn.execute("SELECT COUNT(*) FROM github_events").fetchone()
            checks['total_events'] = result[0] if result else 0
            
            # Check 2: Events by type
            type_counts = self.conn.execute("""
                SELECT event_type, COUNT(*) as count
                FROM github_events
                GROUP BY event_type
                ORDER BY count DESC
            """).fetchall()
            checks['events_by_type'] = dict(type_counts)
            
            # Check 3: Date range
            date_range = self.conn.execute("""
                SELECT MIN(created_at), MAX(created_at)
                FROM github_events
            """).fetchone()
            checks['date_range'] = date_range
            
            # Check 4: Unique values
            unique_counts = self.conn.execute("""
                SELECT 
                    COUNT(DISTINCT repo_name) as unique_repos,
                    COUNT(DISTINCT actor_login) as unique_actors,
                    COUNT(DISTINCT event_type) as unique_event_types
                FROM github_events
            """).fetchone()
            checks['unique_counts'] = {
                'repos': unique_counts[0],
                'actors': unique_counts[1],
                'event_types': unique_counts[2]
            }
            
            logger.info(f"Quality checks completed: {checks['total_events']:,} total events")
            return checks
            
        except Exception as e:
            logger.error(f"Error running quality checks: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

def main():
    """Main processing function"""
    logger.info("Starting data processing...")
    
    try:
        # Initialize processor
        processor = DataProcessor()
        
        # Connect to database
        if not processor.connect():
            return False
        
        # Setup database schema
        if not processor.setup_database():
            return False
        
        # Load raw data
        from ingestion.data_collector import DataCollector
        collector = DataCollector(target_records=100000)
        events = collector.collect_sample_data()
        
        if not events:
            logger.error("No events to process")
            return False
        
        # Process events
        df_processed = processor.process_events(events)
        
        if df_processed.empty:
            logger.error("No events processed successfully")
            return False
        
        # Load to DuckDB
        if not processor.load_to_duckdb(df_processed):
            return False
        
        # Run quality checks
        quality_checks = processor.run_quality_checks()
        
        logger.info("Data processing complete")
        processor.close()
        return quality_checks
        
    except Exception as e:
        logger.error(f"Data processing failed: {e}")
        return False

if __name__ == "__main__":
    # Setup logging
    from utils.logger import setup_logging
    setup_logging()
    
    main()