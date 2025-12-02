# orchestration/pipeline.py (create this file)
import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from prefect import flow, task
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@task
def run_collection():
    from ingestion.fast_collect import fetch_fast_sample_simple
    return fetch_fast_sample_simple(50000)

@task
def run_processing(events):
    from processing.data_processor import DataProcessor
    
    processor = DataProcessor()
    processor.connect()
    processor.setup_database()
    df = processor.process_events(events)
    processor.load_to_duckdb(df, batch_size=50000)
    checks = processor.run_quality_checks()
    processor.close()
    return checks

@task
def run_analysis():
    from analysis.data_analyzer import DataAnalyzer
    
    analyzer = DataAnalyzer()
    analyzer.connect()
    
    results = {
        'basic_statistics': analyzer.get_basic_statistics(),
        'event_types': analyzer.analyze_event_types(),
        'top_repositories': analyzer.analyze_top_repositories(),
        'package_comparison': analyzer.compare_packages([
            'pandas', 'numpy', 'matplotlib', 'pytorch', 'tensorflow'
        ]),
        'insights': analyzer.generate_insights()
    }
    
    analyzer.close()
    return results

@task
def run_visualization(results):
    from visualization.plot_generator import PlotGenerator
    generator = PlotGenerator()
    generator.generate_all_plots(results)
    return True

@flow(name="github-pipeline")
def main_flow():
    print("Starting GitHub Analysis Pipeline with Prefect...")
    logger.info("Pipeline started")
    
    # Run pipeline steps
    events = run_collection()
    quality = run_processing(events)
    analysis = run_analysis()
    run_visualization(analysis)
    
    print("Pipeline complete!")
    logger.info("Pipeline completed successfully")
    return True

if __name__ == "__main__":
    main_flow()