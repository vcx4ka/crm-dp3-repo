# main.py - Main pipeline orchestrator
import logging
import time
from datetime import datetime
import os
from utils.logger import setup_logging
from utils.helpers import time_function

logger = logging.getLogger(__name__)

@time_function
def run_ingestion():
    """Run data ingestion using fast collection method"""
    logger.info("STEP 1: DATA INGESTION")
    print("Starting data ingestion...")
    
    try:
        # Try fast collection first
        from ingestion.fast_collect import fetch_fast_sample
        
        logger.info("Using fast data collection method...")
        events = fetch_fast_sample()
        
        if events:
            # Save the data
            from ingestion.data_collector import DataCollector
            collector = DataCollector()
            output_file = collector.save_raw_data(events)
            logger.info(f"Ingestion complete: {len(events):,} events")
            print(f"Data saved to: {output_file}")
            return events
        else:
            # Fall back to original method
            logger.info("Fast collection failed, using standard method...")
            from ingestion.data_collector import DataCollector
            
            collector = DataCollector(target_records=50000)  # Reduced for speed
            events = collector.collect_sample_data()
            
            if events:
                output_file = collector.save_raw_data(events)
                logger.info(f"Ingestion complete: {len(events):,} events")
                print("Ingestion complete!")
                return events
            else:
                logger.error("Ingestion failed: No events collected")
                print("Ingestion failed: No events collected.")
                return None
                
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        print("Ingestion failed.")
        return None
    
@time_function
def run_processing(events):
    """Run data processing"""
    logger.info("STEP 2: DATA PROCESSING")
    print("Starting data processing...")
    
    try:
        from processing.data_processor import DataProcessor
        
        processor = DataProcessor()
        
        if not processor.connect():
            return None
        
        if not processor.setup_database():
            return None
        
        # Process events - use larger batch size for speed
        df_processed = processor.process_events(events)
        
        if df_processed.empty:
            logger.error("Processing failed: No events processed")
            return None
        
        # Load to DuckDB with larger batch size
        if not processor.load_to_duckdb(df_processed, batch_size=50000):  # Increased from 10000
            return None
        
        # Run quality checks
        quality_checks = processor.run_quality_checks()
        
        processor.close()
        
        logger.info(f"Processing complete: {len(df_processed):,} events loaded")
        print("Processing complete!")
        return quality_checks
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        print("Processing failed.")
        return None

@time_function
def run_analysis():
    """Run data analysis"""
    logger.info("STEP 3: DATA ANALYSIS")
    print("Starting data analysis...")   

    try:
        from analysis.data_analyzer import DataAnalyzer
        
        analyzer = DataAnalyzer()
        
        if not analyzer.connect():
            return None
        
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
        
        analyzer.close()
        
        # Print key insights
        insights = results.get('insights', {})
        if insights:
            print("\nData Insights:\n")
            print(f"Total events: {insights.get('scale', {}).get('total_events', 0):,}")
            
            if 'most_common_event' in insights:
                event = insights['most_common_event']
                print(f"Most common: {event['type']} ({event['percentage']:.1f}%)")
            
            if 'most_active_repo' in insights:
                repo = insights['most_active_repo']
                print(f"Most active: {repo['name']} ({repo['events']:,} events)")
        
        logger.info("Analysis complete!")
        print("Analysis complete!")
        return results
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        print("Analysis failed.")
        return None

@time_function
def run_visualization(analysis_results):
    """Run visualization"""
    logger.info("STEP 4: VISUALIZATION")
    print("Starting visualization...")

    try:
        from visualization.plot_generator import PlotGenerator
        
        generator = PlotGenerator()
        plot_files = generator.generate_all_plots(analysis_results)
        
        print("\nGenerated Visualizations:\n")

        for plot_name, plot_path in plot_files.items():
            if plot_path:
                print(f"    {plot_name.replace('_', ' ').title()}")
        
        logger.info("Visualization complete")
        print("Visualization complete!")
        return plot_files
        
    except Exception as e:
        logger.error(f"Visualization failed: {e}")
        print("Visualization failed.")
        return None

def main():
    """Main pipeline execution"""

    print("\nStarting Github Events Analysis Pipeline...\n")
    logger.info("GitHub Events Analysis Pipeline Starting")
    
    # Setup logging
    log_file = setup_logging(log_to_file=True)
    logger.info("Pipeline started")
    
    # Create directories
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/duckdb", exist_ok=True)
    os.makedirs("data/processed", exist_ok=True)
    os.makedirs("visualizations", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    
    start_time = time.time()
    
    try:
        # Step 1: Ingestion
        events = run_ingestion()
        if not events:
            logger.error("Pipeline failed at ingestion")
            print("Pipeline failed at ingestion")
            return False
        
        # Step 2: Processing
        quality_checks = run_processing(events)
        if not quality_checks:
            logger.error("Pipeline failed at processing")
            print("Pipeline failed at processing")
            return False
        
        # Step 3: Analysis
        analysis_results = run_analysis()
        if not analysis_results:
            logger.error("Pipeline failed at analysis")
            print("Pipeline failed at analysis")
            return False
        
        # Step 4: Visualization
        plot_files = run_visualization(analysis_results)
        if not plot_files:
            logger.error("Pipeline failed at visualization")
            print("Pipeline failed at visualization")
            return False
        
        # Calculate total time
        total_time = time.time() - start_time
        
        # Final summary
        print("\nPipeline ran successfully! Summary...\n")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Total events: {analysis_results.get('basic_statistics', {}).get('total_events', 0):,}")
        print(f"Database: data/duckdb/github_events.db")
        print(f"Visualizations: visualizations/ folder")
        
        return True
        
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user")
        return False
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    
    if success:
        print("\nPipeline completed successfully! Plots available in the visualizations folder.")
    else:
        print("\nPipeline failed. Check logs for details.")
    
    input("\nPress Enter to exit...")