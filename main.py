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

STATE_FILE = "extraction_state.pkl"
LOCK_FILE = "mp_land_scraper.lock"

def create_lock_file():
    try:
        with open(LOCK_FILE, 'w') as f:
            f.write(f"{os.getpid()},{time.time()}")
        logger.info(f"Created lock file with PID {os.getpid()}")
    except Exception as e:
        logger.error(f"Error creating lock file: {str(e)}")

def remove_lock_file():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            logger.info("Removed lock file on exit")
    except Exception as e:
        logger.error(f"Error removing lock file: {str(e)}")

def signal_handler(signum, frame):
    logger.warning(f"Received signal {signum}, exiting gracefully")
    remove_lock_file()
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)
atexit.register(remove_lock_file)

def create_session_with_retries():
    retry_strategy = Retry(
        total=5,
        backoff_factor=1.5,  
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def get_user_agent():
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
    if Path(STATE_FILE).exists():
        try:
            with open(STATE_FILE, 'rb') as f:
                state = pickle.load(f)
            logger.info(f"Loaded state: {len(state.get('completed_districts', []))} districts already processed")
            return state
        except Exception as e:
            logger.error(f"Error loading state: {str(e)}")
    
    return {
        'completed_districts': set(),
        'valid_districts': set(),
        'failed_districts': set(),
        'last_run': None
    }

def save_state(state):
    state['last_run'] = datetime.now()
    try:
        with open(STATE_FILE, 'wb') as f:
            pickle.dump(state, f)
        logger.info("State saved successfully")
    except Exception as e:
        logger.error(f"Error saving state: {str(e)}")

def check_district_validity(district_id, state):
    district_id = str(district_id)
    
    if district_id in state['valid_districts']:
        logger.info(f"District {district_id}: Already verified as valid (from state)")
        return district_id, True
    
    if district_id in state['completed_districts']:
        logger.info(f"District {district_id}: Already fully processed (from state)")
        return district_id, True
    
    url = (
        "https://mpbhulekh.gov.in/gisS_proxyURL.do?"
        "http%3A%2F%2F10.115.250.94%3A8091%2Fgeoserver%2Fows%3Fservice%3DWFS"
        "%26version%3D1.0.0%26request%3DGetFeature%26srsName%3DEPSG%3A4326"
        "%26geometryName%3DGEOM%26typeName%3Dmpwork%3AMS_KHASRA_GEOM"
        "%26filter%3D%3CFilter%3E%3CPropertyIsEqualTo%3E%3CPropertyName%3EDISTRICT_ID%3C%2FPropertyName%3E"
        f"%3CLiteral%3E{district_id}%3C%2FLiteral%3E%3C%2FPropertyIsEqualTo%3E%3C%2FFilter%3E"
        "%26outputFormat%3Djson"
        "%26maxFeatures%3D1"  
    )

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
        response = session.get(url, headers=headers, timeout=20)  
        
        if response.status_code == 200:
            try:
                data = response.json()
                feature_count = len(data.get('features', []))
                
                if feature_count > 0:
                    logger.info(f"District {district_id}: Valid (has features)")
                    state['valid_districts'].add(district_id)
                    save_state(state)
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
    district_id = str(district_id)
    
    if district_id in state['completed_districts']:
        logger.info(f"District {district_id}: Already processed (from state)")
        return district_id, True, 0
    
    url = (
        "https://mpbhulekh.gov.in/gisS_proxyURL.do?"
        "http%3A%2F%2F10.115.250.94%3A8091%2Fgeoserver%2Fows%3Fservice%3DWFS"
        "%26version%3D1.0.0%26request%3DGetFeature%26srsName%3DEPSG%3A4326"
        "%26geometryName%3DGEOM%26typeName%3Dmpwork%3AMS_KHASRA_GEOM"
        "%26filter%3D%3CFilter%3E%3CPropertyIsEqualTo%3E%3CPropertyName%3EDISTRICT_ID%3C%2FPropertyName%3E"
        f"%3CLiteral%3E{district_id}%3C%2FLiteral%3E%3C%2FPropertyIsEqualTo%3E%3C%2FFilter%3E"
        "%26outputFormat%3Djson"
    )

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
        response = session.get(url, headers=headers, timeout=300)
        
        if response.status_code == 200:
            with open(temp_file, "wb") as f:
                f.write(response.content)
            
            try:
                with open(temp_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                
                feature_count = len(data.get('features', []))
                logger.info(f"District {district_id}: Successfully fetched {feature_count} features")
                
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4)
                
                state['completed_districts'].add(district_id)
                save_state(state)
                
                temp_file.unlink(missing_ok=True)
                
                logger.info(f"District {district_id}: Data saved to {output_file}")
                return district_id, True, feature_count
                    
            except json.JSONDecodeError:
                logger.warning(f"District {district_id}: Response is not valid JSON, saving raw content")
                
                os.rename(temp_file, output_file)
                logger.info(f"District {district_id}: Raw response saved to {output_file}")
                
                state['completed_districts'].add(district_id)
                save_state(state)
                
                return district_id, True, 0
        else:
            logger.warning(f"District {district_id}: Error {response.status_code}")
            state['failed_districts'].add(district_id)
            save_state(state)
            return district_id, False, 0
            
    except requests.exceptions.RequestException as e:
        logger.error(f"District {district_id}: Request failed: {str(e)}")
        state['failed_districts'].add(district_id)
        save_state(state)
        return district_id, False, 0

def main():
    logger.info("Starting MP Land data extraction")
    
    create_lock_file()
    
    state = load_state()
    
    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)
    
    min_district = 1
    max_district = 100
    
    logger.info("Step 1: Checking valid districts...")
    valid_districts = list(state['valid_districts'])
    
    for district_id in range(min_district, max_district + 1):
        district_id_str = str(district_id)
        if district_id_str in state['completed_districts'] or district_id_str in state['valid_districts']:
            if district_id_str not in valid_districts:
                valid_districts.append(district_id_str)
            continue
            
        district_id, is_valid = check_district_validity(district_id, state)
        if is_valid and district_id not in valid_districts:
            valid_districts.append(district_id)
            
        time.sleep(random.uniform(1.5, 4.0))
    
    logger.info(f"Found {len(valid_districts)} valid districts: {valid_districts}")
    
    districts_to_download = [d for d in valid_districts if d not in state['completed_districts']]
    logger.info(f"Step 2: Downloading full data for {len(districts_to_download)} remaining districts...")
    
    if not districts_to_download:
        logger.info("All valid districts have already been processed. Nothing to download.")
        return
    
    max_workers = min(8, len(districts_to_download))
    logger.info(f"Using {max_workers} worker threads for parallel downloads")
    
    download_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_district = {
            executor.submit(fetch_district_data, district_id, state): district_id 
            for district_id in districts_to_download
        }
        
        for future in concurrent.futures.as_completed(future_to_district):
            district_id = future_to_district[future]
            try:
                result = future.result()
                download_results.append(result)
                time.sleep(random.uniform(0.8, 2.0))
            except Exception as e:
                logger.error(f"District {district_id} download failed with error: {str(e)}")
                state['failed_districts'].add(district_id)
                save_state(state)
    
    successful_downloads = [r for r in download_results if r[1]]
    logger.info(f"Successfully downloaded data for {len(successful_downloads)} out of {len(districts_to_download)} districts")
    
    for district_id, success, feature_count in successful_downloads:
        logger.info(f"District {district_id}: {feature_count} features")
    
    failed_downloads = [r for r in download_results if not r[1]]
    if failed_downloads:
        failed_ids = [r[0] for r in failed_downloads]
        logger.warning(f"Failed to download data for {len(failed_downloads)} districts: {failed_ids}")
    
    retry_districts = list(state['failed_districts'] - set([d[0] for d in successful_downloads]))
    if retry_districts:
        logger.info(f"Retrying {len(retry_districts)} previously failed districts")
    
    logger.info("Data extraction completed")
    logger.info(f"Total districts processed: {len(state['completed_districts'])}")
    logger.info(f"Run timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()