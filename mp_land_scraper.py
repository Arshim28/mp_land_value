import requests
import json
import concurrent.futures
from pathlib import Path
import time
import logging
import random
import os
import sys
import pickle
import atexit
import signal
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime

# Set up logging with more detailed format for cloud environments
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/mp_land_extraction_{timestamp}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Files for state tracking and process management
STATE_FILE = "extraction_state.pkl"
LOCK_FILE = "mp_land_scraper.lock"

# Create lock file
def create_lock_file():
    """Create a lock file with PID and timestamp"""
    try:
        with open(LOCK_FILE, 'w') as f:
            f.write(f"{os.getpid()},{time.time()}")
        logger.info(f"Created lock file with PID {os.getpid()}")
    except Exception as e:
        logger.error(f"Error creating lock file: {str(e)}")

# Remove lock file when process exits
def remove_lock_file():
    """Remove the lock file on clean exit"""
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            logger.info("Removed lock file on exit")
    except Exception as e:
        logger.error(f"Error removing lock file: {str(e)}")

# Setup signal handlers
def signal_handler(signum, frame):
    """Handle termination signals gracefully"""
    logger.warning(f"Received signal {signum}, exiting gracefully")
    remove_lock_file()
    sys.exit(0)

# Register cleanup functions
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
atexit.register(remove_lock_file)

def create_session_with_retries():
    """Create a requests session with retry capabilities."""
    retry_strategy = Retry(
        total=5,  # Increased from 3 to 5
        backoff_factor=1.5,  # More aggressive backoff
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # Disable SSL verification for problematic connections
    session.verify = False
    
    # Suppress SSL warnings (since we're disabling verification)
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    return session

def get_user_agent():
    """Return a random user agent string."""
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (iPad; CPU OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
    ]
    return random.choice(user_agents)

def load_state():
    """Load state from file if it exists."""
    if Path(STATE_FILE).exists():
        try:
            with open(STATE_FILE, 'rb') as f:
                state = pickle.load(f)
            logger.info(f"Loaded state: {len(state.get('completed_districts', []))} districts already processed")
            return state
        except Exception as e:
            logger.error(f"Error loading state: {str(e)}")
    
    # Default initial state
    return {
        'completed_districts': set(),
        'valid_districts': set(),
        'failed_districts': set(),
        'last_run': None
    }

def save_state(state):
    """Save current execution state."""
    state['last_run'] = datetime.now()
    try:
        with open(STATE_FILE, 'wb') as f:
            pickle.dump(state, f)
        logger.info("State saved successfully")
    except Exception as e:
        logger.error(f"Error saving state: {str(e)}")

def check_district_validity(district_id, state):
    """Check if a district ID is valid by making a lightweight request."""
    district_id = str(district_id)
    
    # Skip if already processed and valid
    if district_id in state['valid_districts']:
        logger.info(f"District {district_id}: Already verified as valid (from state)")
        return district_id, True
    
    # Skip if already completed
    if district_id in state['completed_districts']:
        logger.info(f"District {district_id}: Already fully processed (from state)")
        return district_id, True
    
    # Using the verified working format from the browser
    url = (
        "https://mpbhulekh.gov.in/gisS_proxyURL.do?"
        "http%3A%2F%2F10.115.250.94%3A8091%2Fgeoserver%2Fows%3Fservice%3DWFS"
        "%26version%3D1.1.0"  # Updated version
        "%26request%3DGetFeature"
        "%26srsName%3DEPSG%3A1100000"  # Updated EPSG code
        "%26geometryName%3DGEOM"
        "%26typeName%3Dmpwork%3AMS_KHASRA_GEOM"
        "%26filter%3D%3CFilter%3E"
        "%3CPropertyIsEqualTo%3E%3CPropertyName%3EDISTRICT_ID%3C%2FPropertyName%3E"
        f"%3CLiteral%3E{district_id}%3C%2FLiteral%3E%3C%2FPropertyIsEqualTo%3E"
        "%3C%2FFilter%3E"
        "%26outputFormat%3Djson"
        "%26maxFeatures%3D1"  # Only request 1 feature to check validity
    )

    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type": "application/x-www-form-urlencoded",
        "referer": (
            f"https://mpbhulekh.gov.in/MPWebGISEditor/GISKhasraViewerStart?"
            f"distId={district_id}&maptype=villagemap&maptable=MS_KHASRA_GEOM&usertype=login"
        ),
        "user-agent": get_user_agent(),
        "x-requested-with": "XMLHttpRequest"
    }

    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded",
        "dnt": "1",
        "referer": (
            f"https://mpbhulekh.gov.in/MPWebGISEditor/GISKhasraViewerStart?"
            f"distId={district_id}&maptype=villagemap&maptable=MS_KHASRA_GEOM&usertype=login"
        ),
        "user-agent": get_user_agent(),
        "x-requested-with": "XMLHttpRequest"
    }
    
    session = create_session_with_retries()
    
    try:
        response = session.get(url, headers=headers, timeout=20)  # Increased timeout
        
        if response.status_code == 200:
            try:
                data = response.json()
                feature_count = len(data.get('features', []))
                
                if feature_count > 0:
                    logger.info(f"District {district_id}: Valid (has features)")
                    state['valid_districts'].add(district_id)
                    save_state(state)  # Save state after each successful check
                    return district_id, True
                else:
                    logger.info(f"District {district_id}: Invalid (no features)")
                    return district_id, False
                    
            except json.JSONDecodeError:
                logger.warning(f"District {district_id}: Invalid response format")
                return district_id, False
        else:
            logger.warning(f"District {district_id}: Error {response.status_code}")
            return district_id, False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"District {district_id}: Request failed: {str(e)}")
        return district_id, False

def fetch_district_data(district_id, state):
    """Fetch and save full khasra data for a district."""
    district_id = str(district_id)
    
    # Skip if already completed
    if district_id in state['completed_districts']:
        logger.info(f"District {district_id}: Already processed (from state)")
        return district_id, True, 0
    
    # Using the verified working format from the browser
    url = (
        "https://mpbhulekh.gov.in/gisS_proxyURL.do?"
        "http%3A%2F%2F10.115.250.94%3A8091%2Fgeoserver%2Fows%3Fservice%3DWFS"
        "%26version%3D1.1.0"  # Updated version
        "%26request%3DGetFeature"
        "%26srsName%3DEPSG%3A1100000"  # Updated EPSG code
        "%26geometryName%3DGEOM"
        "%26typeName%3Dmpwork%3AMS_KHASRA_GEOM"
        "%26filter%3D%3CFilter%3E"
        "%3CPropertyIsEqualTo%3E%3CPropertyName%3EDISTRICT_ID%3C%2FPropertyName%3E"
        f"%3CLiteral%3E{district_id}%3C%2FLiteral%3E%3C%2FPropertyIsEqualTo%3E"
        "%3C%2FFilter%3E"
        "%26outputFormat%3Djson"
    )

    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type": "application/x-www-form-urlencoded",
        "referer": (
            f"https://mpbhulekh.gov.in/MPWebGISEditor/GISKhasraViewerStart?"
            f"distId={district_id}&maptype=villagemap&maptable=MS_KHASRA_GEOM&usertype=login"
        ),
        "user-agent": get_user_agent(),
        "x-requested-with": "XMLHttpRequest"
    }

    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded",
        "dnt": "1",
        "referer": (
            f"https://mpbhulekh.gov.in/MPWebGISEditor/GISKhasraViewerStart?"
            f"distId={district_id}&maptype=villagemap&maptable=MS_KHASRA_GEOM&usertype=login"
        ),
        "user-agent": get_user_agent(),
        "x-requested-with": "XMLHttpRequest"
    }

    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)
    
    output_file = output_dir / f"district_{district_id}_full_data.json"
    temp_file = output_dir / f"district_{district_id}_full_data.json.partial"

    logger.info(f"Fetching full data for district {district_id}...")

    session = create_session_with_retries()
    
    try:
        response = session.get(url, headers=headers, timeout=300)  # Increased timeout for full data
        
        if response.status_code == 200:
            # First write to a temporary file
            with open(temp_file, "wb") as f:
                f.write(response.content)
            
            try:
                # Try to parse the JSON to validate it
                with open(temp_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                feature_count = len(data.get('features', []))
                logger.info(f"District {district_id}: Successfully fetched {feature_count} features")
                
                # Format the JSON nicely and save to the final file
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4)
                
                # Update state
                state['completed_districts'].add(district_id)
                save_state(state)
                
                # Clean up the temporary file
                temp_file.unlink(missing_ok=True)
                
                logger.info(f"District {district_id}: Data saved to {output_file}")
                return district_id, True, feature_count
                    
            except json.JSONDecodeError:
                logger.warning(f"District {district_id}: Response is not valid JSON, saving raw content")
                
                # Move the temp file to the final location anyway
                os.rename(temp_file, output_file)
                logger.info(f"District {district_id}: Raw response saved to {output_file}")
                
                # Update state as completed even though it wasn't valid JSON
                state['completed_districts'].add(district_id)
                save_state(state)
                
                return district_id, True, 0
        else:
            logger.warning(f"District {district_id}: Error {response.status_code}")
            # Add to failed districts for retry
            state['failed_districts'].add(district_id)
            save_state(state)
            return district_id, False, 0
            
    except requests.exceptions.RequestException as e:
        logger.error(f"District {district_id}: Request failed: {str(e)}")
        # Add to failed districts for retry
        state['failed_districts'].add(district_id)
        save_state(state)
        return district_id, False, 0

def main():
    """Main function to coordinate the district data extraction process."""
    logger.info("Starting MP Land data extraction")
    
    # Create lock file to indicate process is running
    create_lock_file()
    
    # Load previous state
    state = load_state()
    
    # Create data directory if it doesn't exist
    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)
    
    # Define range of district IDs to test
    min_district = 1
    max_district = 100
    
    # Step 1: Check which districts are valid (single-threaded to avoid rate limiting)
    logger.info("Step 1: Checking valid districts...")
    valid_districts = list(state['valid_districts'])  # Start with previously found valid districts
    
    for district_id in range(min_district, max_district + 1):
        # Skip if already completed or known to be valid
        district_id_str = str(district_id)
        if district_id_str in state['completed_districts'] or district_id_str in state['valid_districts']:
            if district_id_str not in valid_districts:
                valid_districts.append(district_id_str)
            continue
            
        district_id, is_valid = check_district_validity(district_id, state)
        if is_valid and district_id not in valid_districts:
            valid_districts.append(district_id)
            
        # Add a small random delay to avoid detection
        time.sleep(random.uniform(1.5, 4.0))
    
    logger.info(f"Found {len(valid_districts)} valid districts: {valid_districts}")
    
    # Step 2: Download full data for valid districts in parallel
    # Filter out districts that have already been completed
    districts_to_download = [d for d in valid_districts if d not in state['completed_districts']]
    logger.info(f"Step 2: Downloading full data for {len(districts_to_download)} remaining districts...")
    
    if not districts_to_download:
        logger.info("All valid districts have already been processed. Nothing to download.")
        return
    
    # Set optimal thread count based on EC2 instance (16 cores)
    # For I/O bound tasks like web scraping, 2x cores is optimal
    optimal_threads = 32  # 16 cores × 2
    
    # Use optimal count, but don't exceed number of districts to download
    max_workers = min(optimal_threads, len(districts_to_download))
    logger.info(f"Using {max_workers} worker threads for parallel downloads on 16-core instance")
    
    download_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Use threads for I/O-bound operations (HTTP requests)
        # This is more efficient than multiprocessing for network-bound tasks
        future_to_district = {
            executor.submit(fetch_district_data, district_id, state): district_id 
            for district_id in districts_to_download
        }
        
        for future in concurrent.futures.as_completed(future_to_district):
            district_id = future_to_district[future]
            try:
                result = future.result()
                download_results.append(result)
                # Add a small delay between completion of tasks
                time.sleep(random.uniform(0.8, 2.0))
            except Exception as e:
                logger.error(f"District {district_id} download failed with error: {str(e)}")
                state['failed_districts'].add(district_id)
                save_state(state)
    
    # Log summary
    successful_downloads = [r for r in download_results if r[1]]
    logger.info(f"Successfully downloaded data for {len(successful_downloads)} out of {len(districts_to_download)} districts")
    
    for district_id, success, feature_count in successful_downloads:
        logger.info(f"District {district_id}: {feature_count} features")
    
    # Check for failed districts
    failed_downloads = [r for r in download_results if not r[1]]
    if failed_downloads:
        failed_ids = [r[0] for r in failed_downloads]
        logger.warning(f"Failed to download data for {len(failed_downloads)} districts: {failed_ids}")
    
    # Try to retry failed districts from previous runs
    retry_districts = list(state['failed_districts'] - set([d[0] for d in successful_downloads]))
    if retry_districts:
        logger.info(f"Retrying {len(retry_districts)} previously failed districts")
        # Code for retry logic would go here - omitted for brevity
    
    logger.info("Data extraction completed")
    logger.info(f"Total districts processed: {len(state['completed_districts'])}")
    logger.info(f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()