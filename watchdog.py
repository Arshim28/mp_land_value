import os
import sys
import time
import subprocess
import logging
import re
import pickle
from pathlib import Path
from datetime import datetime, timedelta

log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [WATCHDOG] - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/watchdog_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

SCRIPT_NAME = "mp_land_scraper.py"
SCRIPT_PATH = Path(__file__).parent / SCRIPT_NAME
STATE_FILE = "extraction_state.pkl"
LOCK_FILE = "mp_land_scraper.lock"
MAX_RUNTIME_HOURS = 12

def is_process_running():
    try:
        result = subprocess.run(
            ["pgrep", "-f", f"python.*{SCRIPT_NAME}"], 
            capture_output=True, 
            text=True
        )
        
        pids = result.stdout.strip().split('\n')
        current_pid = str(os.getpid())
        
        running_pids = [pid for pid in pids if pid and pid != current_pid]
        
        if running_pids:
            logger.info(f"Script is running with PIDs: {', '.join(running_pids)}")
            return True
        else:
            logger.info("Script is not currently running")
            return False
    except Exception as e:
        logger.error(f"Error checking process status: {str(e)}")
        return False

def check_lock_file():
    lock_path = Path(LOCK_FILE)
    
    if not lock_path.exists():
        logger.info("No lock file found")
        return False, 0
    
    try:
        with open(lock_path, 'r') as f:
            content = f.read().strip().split(',')
            if len(content) >= 2:
                pid = int(content[0])
                timestamp = float(content[1])
                
                if subprocess.run(['ps', '-p', str(pid)], capture_output=True).returncode != 0:
                    age_hours = (time.time() - timestamp) / 3600
                    if age_hours > MAX_RUNTIME_HOURS:
                        logger.warning(f"Found stale lock file (age: {age_hours:.1f} hours)")
                        return True, pid
                    else:
                        logger.info(f"Lock file exists but process recently ended (age: {age_hours:.1f} hours)")
                else:
                    logger.info(f"Lock file exists and process {pid} is running")
                    return False, pid 
    except Exception as e:
        logger.error(f"Error reading lock file: {str(e)}")
    
    return False, 0

def check_recent_logs():
    try:
        log_files = sorted(log_dir.glob("mp_land_extraction_*.log"), reverse=True)
        
        if not log_files:
            logger.warning("No log files found to check")
            return False, "No logs"
        
        with open(log_files[0], 'r') as f:
            content = f.read()
            
            if "Data extraction completed" in content and "Total districts processed:" in content:
                logger.info("Script completed successfully according to logs")
                return True, "Completed"
            
            error_patterns = [
                "Traceback \\(most recent call last\\)",
                "Error: .+Exception",
                "Fatal error",
                "Killed"
            ]
            
            for pattern in error_patterns:
                if re.search(pattern, content):
                    logger.warning(f"Found error pattern in logs: {pattern}")
                    return False, "Crashed"
            
            last_timestamp = None
            last_line = None
            for line in content.splitlines():
                if re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", line):
                    time_str = line.split(" - ")[0]
                    try:
                        last_timestamp = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S,%f")
                        last_line = line
                    except:
                        pass
            
            if last_timestamp:
                time_diff = datetime.now() - last_timestamp
                if time_diff > timedelta(hours=1):
                    logger.warning(f"Log hasn't been updated for {time_diff.total_seconds()/3600:.1f} hours")
                    logger.warning(f"Last log entry: {last_line}")
                    return False, "Stalled"
                
            logger.info("Log analysis inconclusive, assuming still in progress")
            return True, "In Progress"
                
    except Exception as e:
        logger.error(f"Error checking log files: {str(e)}")
        return False, f"Error: {str(e)}"

def should_restart():
    if is_process_running():
        return False

    is_stale, pid = check_lock_file()
    if is_stale:
        logger.warning(f"Detected stale lock file for PID {pid}")
        try:
            os.remove(LOCK_FILE)
            logger.info("Removed stale lock file")
        except:
            logger.error("Failed to remove stale lock file")
        return True
    
    success, status = check_recent_logs()
    if not success and status != "Completed":
        logger.warning(f"Script appears to be in state: {status}")
        return True
    
    try:
        if Path(STATE_FILE).exists():
            state_modified = datetime.fromtimestamp(os.path.getmtime(STATE_FILE))
            hours_since_modified = (datetime.now() - state_modified).total_seconds() / 3600
            
            if hours_since_modified > 24:
                logger.warning(f"State file hasn't been updated in {hours_since_modified:.1f} hours")
                return True
    except Exception as e:
        logger.error(f"Error checking state file: {str(e)}")
    
    return False

def create_lock_file():
    try:
        with open(LOCK_FILE, 'w') as f:
            f.write(f"{os.getpid()},{time.time()}")
        logger.info(f"Created lock file with PID {os.getpid()}")
        return True
    except Exception as e:
        logger.error(f"Failed to create lock file: {str(e)}")
        return False

def start_script():
    try:
        logger.info(f"Starting {SCRIPT_NAME}...")
        process = subprocess.Popen(
            [sys.executable, SCRIPT_PATH],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True
        )
        logger.info(f"Started script with PID {process.pid}")
        return True
    except Exception as e:
        logger.error(f"Failed to start script: {str(e)}")
        return False

def main():
    logger.info("Watchdog started")
    
    if should_restart():
        logger.warning("Script needs to be restarted")
        start_script()
    else:
        logger.info("No restart needed")
    
    logger.info("Watchdog completed")

if __name__ == "__main__":
    main()