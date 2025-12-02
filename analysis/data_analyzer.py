# analysis/data_analyzer.py - Analyze GitHub data
import logging
import duckdb
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from utils.helpers import time_function, format_large_number

logger = logging.getLogger(__name__)

class DataAnalyzer:
    def __init__(self, db_path: str = "data/duckdb/github_events.db"):
        self.db_path = db_path
        self.conn = None
        
    @time_function
    def connect(self):
        """Connect to DuckDB database"""
        try:
            self.conn = duckdb.connect(self.db_path)
            logger.info(f"Connected to DuckDB for analysis: {self.db_path}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to DuckDB: {e}")
            return False
    
    @time_function
    def get_basic_statistics(self) -> Dict[str, Any]:
        """Get basic statistics about the data"""
        logger.info("Calculating basic statistics...")
        
        try:
            stats = {}
            
            # Total events
            result = self.conn.execute("SELECT COUNT(*) FROM github_events").fetchone()
            stats['total_events'] = result[0] if result else 0
            
            # Date range
            result = self.conn.execute("""
                SELECT 
                    MIN(created_at), 
                    MAX(created_at),
                    MAX(created_at) - MIN(created_at) as date_range
                FROM github_events
            """).fetchone()
            
            if result:
                stats['date_range'] = {
                    'start': result[0],
                    'end': result[1],
                    'days': result[2].days if result[2] else 0
                }
            
            # Unique counts
            result = self.conn.execute("""
                SELECT 
                    COUNT(DISTINCT repo_name) as repos,
                    COUNT(DISTINCT actor_login) as actors,
                    COUNT(DISTINCT event_type) as event_types,
                    COUNT(DISTINCT org_login) as orgs
                FROM github_events
            """).fetchone()
            
            if result:
                stats['unique_counts'] = {
                    'repositories': result[0],
                    'actors': result[1],
                    'event_types': result[2],
                    'organizations': result[3]
                }
            
            logger.info(f"Basic statistics: {stats['total_events']:,} events")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting basic statistics: {e}")
            return {}
    
    @time_function
    def analyze_event_types(self, top_n: int = 10) -> pd.DataFrame:
        """Analyze distribution of event types"""
        logger.info("Analyzing event types...")
        
        try:
            query = f"""
                SELECT 
                    event_type,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
                FROM github_events
                GROUP BY event_type
                ORDER BY count DESC
                LIMIT {top_n}
            """
            
            df = self.conn.execute(query).fetchdf()
            logger.info(f"Found {len(df)} event types")
            return df
            
        except Exception as e:
            logger.error(f"Error analyzing event types: {e}")
            return pd.DataFrame()
    
    @time_function
    def analyze_top_repositories(self, top_n: int = 15) -> pd.DataFrame:
        """Analyze top repositories by activity"""
        logger.info("Analyzing top repositories...")
        
        try:
            query = f"""
                SELECT 
                    repo_name,
                    COUNT(*) as total_events,
                    COUNT(DISTINCT event_type) as event_types_count,
                    COUNT(DISTINCT actor_login) as unique_contributors,
                    MIN(created_at) as first_event,
                    MAX(created_at) as last_event,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
                FROM github_events
                WHERE repo_name IS NOT NULL AND repo_name != ''
                GROUP BY repo_name
                ORDER BY total_events DESC
                LIMIT {top_n}
            """
            
            df = self.conn.execute(query).fetchdf()
            logger.info(f"Analyzed {len(df)} repositories")
            return df
            
        except Exception as e:
            logger.error(f"Error analyzing repositories: {e}")
            return pd.DataFrame()
    
    @time_function
    def analyze_top_contributors(self, top_n: int = 20) -> pd.DataFrame:
        """Analyze top contributors"""
        logger.info("Analyzing top contributors...")
        
        try:
            query = f"""
                SELECT 
                    actor_login,
                    COUNT(*) as total_events,
                    COUNT(DISTINCT event_type) as event_types_count,
                    COUNT(DISTINCT repo_name) as repos_contributed_to,
                    MIN(created_at) as first_activity,
                    MAX(created_at) as last_activity
                FROM github_events
                WHERE actor_login IS NOT NULL AND actor_login != ''
                GROUP BY actor_login
                ORDER BY total_events DESC
                LIMIT {top_n}
            """
            
            df = self.conn.execute(query).fetchdf()
            logger.info(f"Analyzed {len(df)} contributors")
            return df
            
        except Exception as e:
            logger.error(f"Error analyzing contributors: {e}")
            return pd.DataFrame()
    
    @time_function
    def analyze_temporal_patterns(self) -> Dict[str, pd.DataFrame]:
        """Analyze temporal patterns (hourly, daily, monthly)"""
        logger.info("Analyzing temporal patterns...")
        
        patterns = {}
        
        try:
            # Hourly patterns
            hourly_query = """
                SELECT 
                    hour_of_day,
                    COUNT(*) as event_count,
                    COUNT(DISTINCT event_type) as unique_event_types
                FROM github_events
                GROUP BY hour_of_day
                ORDER BY hour_of_day
            """
            patterns['hourly'] = self.conn.execute(hourly_query).fetchdf()
            
            # Daily patterns
            daily_query = """
                SELECT 
                    day_of_week,
                    COUNT(*) as event_count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
                FROM github_events
                GROUP BY day_of_week
                ORDER BY 
                    CASE day_of_week
                        WHEN 'Monday' THEN 1
                        WHEN 'Tuesday' THEN 2
                        WHEN 'Wednesday' THEN 3
                        WHEN 'Thursday' THEN 4
                        WHEN 'Friday' THEN 5
                        WHEN 'Saturday' THEN 6
                        WHEN 'Sunday' THEN 7
                    END
            """
            patterns['daily'] = self.conn.execute(daily_query).fetchdf()
            
            # Monthly trends
            monthly_query = """
                SELECT 
                    month,
                    COUNT(*) as event_count,
                    COUNT(DISTINCT repo_name) as unique_repos,
                    COUNT(DISTINCT actor_login) as unique_actors
                FROM github_events
                GROUP BY month
                ORDER BY month
            """
            patterns['monthly'] = self.conn.execute(monthly_query).fetchdf()
            
            logger.info("Temporal patterns analyzed")
            return patterns
            
        except Exception as e:
            logger.error(f"Error analyzing temporal patterns: {e}")
            return {}
    
    @time_function
    def analyze_repository_health(self) -> pd.DataFrame:
        """Analyze repository health metrics"""
        logger.info("Analyzing repository health...")
        
        try:
            query = """
                SELECT 
                    repo_name,
                    COUNT(*) as total_events,
                    COUNT(DISTINCT actor_login) as unique_contributors,
                    COUNT(DISTINCT event_type) as activity_diversity,
                    
                    -- Event type breakdown
                    SUM(CASE WHEN event_type = 'PushEvent' THEN 1 ELSE 0 END) as push_events,
                    SUM(CASE WHEN event_type = 'IssuesEvent' THEN 1 ELSE 0 END) as issue_events,
                    SUM(CASE WHEN event_type = 'PullRequestEvent' THEN 1 ELSE 0 END) as pr_events,
                    SUM(CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END) as star_events,
                    
                    -- Activity ratios
                    ROUND(
                        SUM(CASE WHEN event_type = 'PushEvent' THEN 1 ELSE 0 END) * 100.0 / 
                        NULLIF(COUNT(*), 0), 2
                    ) as push_percentage,
                    
                    ROUND(
                        SUM(CASE WHEN event_type IN ('IssuesEvent', 'PullRequestEvent') THEN 1 ELSE 0 END) * 100.0 / 
                        NULLIF(COUNT(*), 0), 2
                    ) as collaboration_percentage,
                    
                    -- Recent activity (last 30 days)
                    SUM(CASE WHEN created_at >= DATE_SUB(CURRENT_DATE, 30) THEN 1 ELSE 0 END) as recent_activity
                    
                FROM github_events
                WHERE repo_name IS NOT NULL
                GROUP BY repo_name
                HAVING total_events >= 10
                ORDER BY total_events DESC
                LIMIT 50
            """
            
            df = self.conn.execute(query).fetchdf()
            logger.info(f"Analyzed health of {len(df)} repositories")
            return df
            
        except Exception as e:
            logger.error(f"Error analyzing repository health: {e}")
            return pd.DataFrame()
        
    @time_function
    def compare_packages(self, package_names: List[str]) -> pd.DataFrame:
        """Compare activity metrics between specific packages"""
        logger.info(f"Comparing {len(package_names)} packages...")
        
        try:
            comparison_data = []
            
            for package in package_names:
                query = f"""
                    SELECT 
                        '{package}' as package_name,
                        COUNT(*) as total_events,
                        COUNT(DISTINCT event_type) as event_types,
                        COUNT(DISTINCT actor_login) as unique_contributors,
                        SUM(CASE WHEN event_type = 'PushEvent' THEN 1 ELSE 0 END) as push_events,
                        SUM(CASE WHEN event_type = 'WatchEvent' THEN 1 ELSE 0 END) as star_events,
                        SUM(CASE WHEN event_type = 'IssuesEvent' THEN 1 ELSE 0 END) as issue_events,
                        SUM(CASE WHEN event_type = 'PullRequestEvent' THEN 1 ELSE 0 END) as pr_events,
                        ROUND(AVG(CASE WHEN event_type = 'PushEvent' THEN 1 ELSE 0 END) * 100, 2) as push_percentage,
                        COUNT(CASE WHEN created_at >= DATE_SUB(CURRENT_DATE, 30) THEN 1 END) as events_last_30_days
                    FROM github_events
                    WHERE LOWER(repo_name) LIKE '%{package}%'
                    OR repo_name LIKE '%{package}-dev%'
                    OR repo_name LIKE '%{package}lib%'
                """
                
                result = self.conn.execute(query).fetchone()
                if result:
                    comparison_data.append(result)
            
            df = pd.DataFrame(comparison_data, columns=[
                'package_name', 'total_events', 'event_types', 'unique_contributors',
                'push_events', 'star_events', 'issue_events', 'pr_events',
                'push_percentage', 'events_last_30_days'
            ])
            
            # Calculate activity velocity (events per day)
            df['events_per_day'] = df['total_events'] / 90  # Assuming 90 days of data
            
            logger.info(f"Comparison complete for {len(df)} packages")
            return df.sort_values('total_events', ascending=False)
            
        except Exception as e:
            logger.error(f"Error comparing packages: {e}")
            return pd.DataFrame()

    @time_function  
    def detect_trends(self):
        """Detect emerging trends in package activity"""
        logger.info("Detecting activity trends...")
        
        try:
            # Calculate weekly activity trends
            query = """
                WITH weekly_activity AS (
                    SELECT 
                        repo_name,
                        DATE_TRUNC('week', created_at) as week_start,
                        COUNT(*) as weekly_events,
                        LAG(COUNT(*), 1) OVER (PARTITION BY repo_name ORDER BY DATE_TRUNC('week', created_at)) as prev_week_events
                    FROM github_events
                    WHERE created_at >= DATE_SUB(CURRENT_DATE, 90)
                    GROUP BY repo_name, DATE_TRUNC('week', created_at)
                ),
                trend_analysis AS (
                    SELECT 
                        repo_name,
                        week_start,
                        weekly_events,
                        prev_week_events,
                        CASE 
                            WHEN prev_week_events IS NULL THEN 0
                            ELSE ROUND((weekly_events - prev_week_events) * 100.0 / prev_week_events, 2)
                        END as growth_percentage,
                        AVG(weekly_events) OVER (PARTITION BY repo_name ORDER BY week_start ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) as moving_avg_4_weeks
                    FROM weekly_activity
                )
                SELECT 
                    repo_name,
                    week_start,
                    weekly_events,
                    growth_percentage,
                    moving_avg_4_weeks,
                    CASE 
                        WHEN growth_percentage > 50 THEN 'Rapid Growth'
                        WHEN growth_percentage > 20 THEN 'Steady Growth' 
                        WHEN growth_percentage < -20 THEN 'Declining'
                        ELSE 'Stable'
                    END as trend_category
                FROM trend_analysis
                WHERE week_start >= DATE_SUB(CURRENT_DATE, 30)
                ORDER BY growth_percentage DESC
                LIMIT 20
            """
            
            df = self.conn.execute(query).fetchdf()
            logger.info(f"Trend detection complete: {len(df)} trends identified")
            return df
            
        except Exception as e:
            logger.error(f"Error detecting trends: {e}")
            return pd.DataFrame()
    
    @time_function
    def generate_insights(self) -> Dict[str, Any]:
        """Generate key insights from the analysis"""
        logger.info("Generating insights...")
        
        insights = {}
        
        try:
            # Get all analysis results
            basic_stats = self.get_basic_statistics()
            event_types = self.analyze_event_types()
            top_repos = self.analyze_top_repositories()
            temporal_patterns = self.analyze_temporal_patterns()
            
            # Generate insights
            insights['scale'] = {
                'total_events': basic_stats.get('total_events', 0),
                'date_range_days': basic_stats.get('date_range', {}).get('days', 0)
            }
            
            # Most common event type
            if not event_types.empty:
                most_common = event_types.iloc[0]
                insights['most_common_event'] = {
                    'type': most_common['event_type'],
                    'count': int(most_common['count']),
                    'percentage': float(most_common['percentage'])
                }
            
            # Busiest hour
            if 'hourly' in temporal_patterns and not temporal_patterns['hourly'].empty:
                busiest_hour = temporal_patterns['hourly'].loc[temporal_patterns['hourly']['event_count'].idxmax()]
                insights['busiest_hour'] = {
                    'hour': int(busiest_hour['hour_of_day']),
                    'event_count': int(busiest_hour['event_count'])
                }
            
            # Most active repository
            if not top_repos.empty:
                most_active = top_repos.iloc[0]
                insights['most_active_repo'] = {
                    'name': most_active['repo_name'],
                    'events': int(most_active['total_events']),
                    'contributors': int(most_active['unique_contributors'])
                }
            
            # Activity distribution
            if 'daily' in temporal_patterns and not temporal_patterns['daily'].empty:
                busiest_day = temporal_patterns['daily'].loc[temporal_patterns['daily']['event_count'].idxmax()]
                insights['busiest_day'] = {
                    'day': busiest_day['day_of_week'],
                    'percentage': float(busiest_day['percentage'])
                }
            
            logger.info(f"Generated {len(insights)} insights")
            return insights
            
        except Exception as e:
            logger.error(f"Error generating insights: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Analysis database connection closed")

def main():
    """Main analysis function"""
    logger.info("Starting data analysis...")
    
    try:
        # Initialize analyzer
        analyzer = DataAnalyzer()
        
        # Connect to database
        if not analyzer.connect():
            return {}
        
        # Run all analyses
        results = {
            'basic_statistics': analyzer.get_basic_statistics(),
            'event_types': analyzer.analyze_event_types(),
            'top_repositories': analyzer.analyze_top_repositories(),
            'top_contributors': analyzer.analyze_top_contributors(),
            'temporal_patterns': analyzer.analyze_temporal_patterns(),
            'repository_health': analyzer.analyze_repository_health(),
            'insights': analyzer.generate_insights()
        }
        
        # Print summary
        if 'insights' in results:
            insights = results['insights']
            print("\nSummary of Data Analysis: \n")
            print(f"Total events: {insights.get('scale', {}).get('total_events', 0):,}")
            
            if 'most_common_event' in insights:
                event = insights['most_common_event']
                print(f"Most common event: {event['type']} ({event['percentage']:.1f}%)")
            
            if 'most_active_repo' in insights:
                repo = insights['most_active_repo']
                print(f"Most active repo: {repo['name']} ({repo['events']:,} events)")
            
            if 'busiest_hour' in insights:
                hour = insights['busiest_hour']
                print(f"Busiest hour: {hour['hour']:02d}:00 ({hour['event_count']:,} events)")
        
        logger.info("Data analysis complete")
        analyzer.close()
        return results
        
    except Exception as e:
        logger.error(f"Data analysis failed: {e}")
        return {}

if __name__ == "__main__":
    # Setup logging
    from utils.logger import setup_logging
    setup_logging()
    
    main()