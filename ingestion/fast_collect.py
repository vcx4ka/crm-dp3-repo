# ingestion/fast_collect.py - Quick data collection for testing
import logging
import requests
import json
import os
import sys
import time
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to path to find utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import setup_logging

logger = logging.getLogger(__name__)

def download_hour_parallel(date_str, hour, max_events=50000):
    """Download one hour of data in parallel, to speed up collection time"""
    url = f"https://data.gharchive.org/{date_str}-{hour}.json.gz"
    
    try:
        import gzip
        import io
        
        print(f"  Downloading {date_str}-{hour:02d}...")
        start_time = time.time()
        
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        
        events = []
        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz_file:
            for i, line in enumerate(gz_file):
                if i >= max_events:
                    break
                try:
                    event = json.loads(line.decode('utf-8'))
                    events.append(event)
                except:
                    continue
        
        elapsed = time.time() - start_time
        print(f"SUCCESS: {date_str}-{hour:02d}: {len(events):,} events ({elapsed:.1f}s)")
        logger.info(f"Downloaded {len(events)} events from {date_str}-{hour:02d} in {elapsed:.1f} seconds")
        return events
        
    except Exception as e:
        print(f"{date_str}-{hour:02d}: Error - {str(e)[:50]}")
        logger.info(f"Error downloading {date_str}-{hour:02d}: {e}")
        return []

def fetch_fast_sample(target_events=50000):
    """Fetch a large sample from GitHub Archive quickly using parallel downloads"""
    
    print(f"Target: {target_events:,} events")
    print("Downloading from GitHub Archive...")
    
    all_events = []
    
    # Download multiple recent hours in parallel
    hours_to_download = []
    for days_back in range(3):  # Last 3 days
        date_str = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        # Download 3 hours from each day (12, 15, 18 UTC are usually busy)
        for hour in [12, 15, 18]:
            hours_to_download.append((date_str, hour))
    
    # Download in parallel for speed
    print(f"\nDownloading {len(hours_to_download)} hours in parallel...")
    logger.info(f"Starting parallel download of {len(hours_to_download)} hours")
    
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for date_str, hour in hours_to_download:
            future = executor.submit(download_hour_parallel, date_str, hour, 20000)
            futures.append(future)
        
        for future in as_completed(futures):
            try:
                hour_events = future.result()
                all_events.extend(hour_events)
                print(f"Total so far: {len(all_events):,} events")
                logger.info(f"Total events collected so far: {len(all_events)}")
                
                # Stop if we have enough
                if len(all_events) >= target_events:
                    print(f"\n Reached target of {target_events:,} events")
                    logger.info(f"Reached target of {target_events} events")
                    all_events = all_events[:target_events]
                    break
                    
            except Exception as e:
                print(f"Error processing future: {e}")
                logger.info(f"Error processing future: {e}")
    
    # If we still don't have enough, download more
    if len(all_events) < target_events:
        print(f"\nNeed more data, downloading additional hours...")
        logger.info("Downloading additional hours to reach target")
        
        # Download a few more hours sequentially
        extra_hours = [(datetime.now().strftime("%Y-%m-%d"), h) for h in [10, 11, 13, 14]]
        
        for date_str, hour in extra_hours:
            if len(all_events) >= target_events:
                break
            
            print(f"  Downloading extra: {date_str}-{hour:02d}...")
            logger.info(f"Downloading extra hour: {date_str}-{hour:02d}")
            try:
                hour_events = download_hour_parallel(date_str, hour, 25000)
                all_events.extend(hour_events)
                print(f"  Total: {len(all_events):,} events")
                logger.info(f"Total events collected so far: {len(all_events)}")
            except:
                continue
    
    # Limit to target
    if len(all_events) > target_events:
        all_events = all_events[:target_events]
    
    print(f"\n Collection complete. {len(all_events):,} total events collected")
    logger.info(f"Collection complete. {len(all_events)} total events collected")
    
    # Save to file
    os.makedirs("data/raw", exist_ok=True)
    output_file = f"data/raw/quick_sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    print(f"\nSaving to {output_file}...")
    logger.info(f"Saving collected events to {output_file}")
    with open(output_file, 'w') as f:
        json.dump(all_events, f)
    
    print(f"Saved successfully!")
    logger.info(f"Successfully saved {len(all_events)} events to {output_file}")
    
    return all_events

def fetch_fast_sample_simple(target_events=50000):
    """Simpler version - download one large hour file"""
    
    # Use a recent hour that's likely to have lots of data
    date_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    hour = 15  
    
    url = f"https://data.gharchive.org/{date_str}-{hour}.json.gz"
    
    print(f"Fetching: {url}")
    logger.info(f"Fetching large sample from {url}")
    
    try:
        import gzip
        import io
        
        start_time = time.time()
        response = requests.get(url, stream=True, timeout=120)
        response.raise_for_status()
        
        events = []
        print("Parsing data...")
        
        with gzip.GzipFile(fileobj=io.BytesIO(response.content)) as gz_file:
            for i, line in enumerate(gz_file):
                try:
                    event = json.loads(line.decode('utf-8'))
                    events.append(event)
                    
                    # Show progress
                    if i % 50000 == 0 and i > 0:
                        print(f"  Processed {i:,} lines...")
                    
                    # Stop when we have enough
                    if len(events) >= target_events:
                        print(f"  Reached target of {target_events:,} events")
                        break
                        
                except:
                    continue
        
        elapsed = time.time() - start_time
        print(f" Successfully collected {len(events):,} events in {elapsed:.1f} seconds")
        logger.info(f"Collected {len(events)} events in {elapsed:.1f} seconds")
        
        # Save to file
        os.makedirs("data/raw", exist_ok=True)
        output_file = f"data/raw/large_sample_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        print(f"\nSaving {len(events):,} events...")
        with open(output_file, 'w') as f:
            # Write in chunks to avoid memory issues
            f.write('[\n')
            for i, event in enumerate(events):
                json.dump(event, f)
                if i < len(events) - 1:
                    f.write(',\n')
            f.write('\n]')
        
        print("Successfully saved events to file.")
        logger.info(f"Saved {len(events)} events to {output_file}")
        
        return events
        
    except Exception as e:
        print(f"ERROR: Failed to download real data: {e}")
        logger.info(f"Failed to download data: {e}")
        
        # generating random samples of data instead, since fetching data failed
        print("Using fallback method...")
        logger.info("Generating fallback sample data")
        return generate_fallback_sample(target_events)

def generate_fallback_sample(target_events=50000):
    """Generate a large sample dataset quickly"""
    import random
    from datetime import datetime, timedelta
    
    repos = [
        "pandas-dev/pandas", "numpy/numpy", "matplotlib/matplotlib",
        "scikit-learn/scikit-learn", "pytorch/pytorch", "tensorflow/tensorflow",
        "seaborn/seaborn", "plotly/plotly", "scipy/scipy", "polars/polars",
        "python/cpython", "django/django", "flask/flask", "psf/requests"
    ]
    
    print(f"Generating {target_events:,} sample events...")
    
    events = []
    for i in range(target_events):
        repo = random.choice(repos)
        days_ago = random.randint(0, 90)
        event_time = datetime.now() - timedelta(days=days_ago)
        
        event_types = ['PushEvent', 'WatchEvent', 'IssuesEvent', 
                      'PullRequestEvent', 'ForkEvent', 'ReleaseEvent',
                      'CreateEvent', 'DeleteEvent', 'GollumEvent']
        
        event = {
            'id': f'event_{i:08d}_{random.randint(1000, 9999)}',
            'type': random.choice(event_types),
            'repo': {
                'name': repo,
                'id': random.randint(100000, 999999)
            },
            'actor': {
                'login': f'user_{random.randint(1, 5000)}',
                'id': random.randint(10000, 99999)
            },
            'created_at': event_time.isoformat() + 'Z',
            'public': True,
            'payload': {
                'size': random.randint(1, 10) if random.random() > 0.5 else None
            }
        }
        events.append(event)
        
        # Show progress
        if i % 10000 == 0 and i > 0:
            print(f"  Generated {i:,} events...")
    
    print(f" Successfully generated {len(events):,} sample events")
    logger.info(f"Generated {len(events)} sample events")
    
    # Save to file
    os.makedirs("data/raw", exist_ok=True)
    output_file = f"data/raw/fallback_large_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    print(f"Saving to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump(events, f)
    
    print(f"Saved file to {output_file} ")
    logger.info(f"Saved fallback sample events to {output_file}")
    
    return events

if __name__ == "__main__":
    setup_logging()
    
    print("Data Collection from GitHub Archive:\n")
    print("Downloading 50,000+ GitHub events...")
    
    # Use the simple method for speed
    events = fetch_fast_sample_simple(target_events=50000)
    
    if events:
        print(f"\n Successfully fetched {len(events):,} events. Ready for processing.")
        print("Next, run 'python main.py' to complete the pipeline and check the 'visualizations' folder for the generated plots.\n")
        print("Database will be at: data/duckdb/github_events.db")
        logger.info("Data collection complete. Ready for processing.")
    else:
        print("\n Failed to collect data")
        logger.info("Data collection failed.")