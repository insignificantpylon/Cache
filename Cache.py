import os
import shutil
import psutil
import json
from pathlib import Path
import subprocess
import logging
from collections import OrderedDict
import time
import threading

# Constants
CONFIG_FILE = "cache_config.json"
LOG_FILE = "cache_manager.log"
TARGET_EXTENSIONS = {".exe", ".dll", ".sys", ".iso"}
DEFAULT_CACHE_PERCENT = 0.5
MIN_CACHE_GB = 10
MAX_CACHE_GB = 500

# Configure logging
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class RAMCache:
    """In-memory cache using RAM with LRU eviction"""
    def __init__(self):
        self.cache = OrderedDict()
        self.max_size = self.get_available_ram()
        
    def get_available_ram(self):
        mem = psutil.virtual_memory()
        return int(mem.available * 0.9)

    def update_max_size(self):
        new_size = self.get_available_ram()
        self.max_size = new_size
        self.trim_cache()
        
    def trim_cache(self):
        current_size = sum(len(data) for data in self.cache.values())
        while current_size > self.max_size and self.cache:
            _, _ = self.cache.popitem(last=False)
            current_size = sum(len(data) for data in self.cache.values())

    def add(self, path, data):
        self.update_max_size()
        if len(data) > self.max_size:
            return False
        self.trim_cache()
        self.cache[path] = data
        self.cache.move_to_end(path)
        return True

    def get(self, path):
        if path in self.cache:
            self.cache.move_to_end(path)
            return self.cache[path]
        return None

    def remove(self, path):
        if path in self.cache:
            del self.cache[path]

class AutoCacheManager:
    def __init__(self, cache_percent=DEFAULT_CACHE_PERCENT, min_gb=MIN_CACHE_GB, max_gb=MAX_CACHE_GB):
        self.cache_percent = cache_percent
        self.min_cache_bytes = min_gb * (1024 ** 3)
        self.max_cache_bytes = max_gb * (1024 ** 3)
        self.running = True  # Start in running state
        self.detect_drives()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.set_initial_cache_size()
        self.load_cache()
        self.ram_cache = RAMCache()
        logging.info(f"Initialized with dynamic cache size: {self.cache_size_bytes / (1024 ** 3):.2f} GB")

    def detect_drives(self):
        self.hdds = []
        self.ssds = []
        for disk in psutil.disk_partitions(all=True):
            drive = disk.device
            try:
                drive_type = subprocess.run(
                    ["wmic", "diskdrive", "where", f"DeviceID='{drive}'", "get", "MediaType"],
                    capture_output=True, text=True, check=True, timeout=5
                ).stdout
                if "SSD" in drive_type:
                    self.ssds.append(drive)
                else:
                    self.hdds.append(drive)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
                logging.warning(f"Could not determine type for {drive}: {e}")

        if not self.ssds or not self.hdds:
            raise Exception("No SSD or HDD detected!")
        self.cache_dir = Path(self.ssds[0]) / "Cache"
        self.hdd_dir = Path(self.hdds[0])
        self.ssd_path = self.ssds[0]
        logging.info(f"SSD cache dir: {self.cache_dir}, HDD dir: {self.hdd_dir}")

    def set_initial_cache_size(self):
        usage = psutil.disk_usage(self.ssd_path)
        free_space_bytes = usage.free
        proposed_size = int(free_space_bytes * self.cache_percent)
        self.cache_size_bytes = max(self.min_cache_bytes, min(self.max_cache_bytes, proposed_size))
        
    def adjust_cache_size(self):
        usage = psutil.disk_usage(self.ssd_path)
        free_space_bytes = usage.free
        if free_space_bytes < usage.total * 0.1:
            new_size = int(free_space_bytes * 0.8)
        else:
            new_size = int(free_space_bytes * self.cache_percent)
        new_size = max(self.min_cache_bytes, min(self.max_cache_bytes, new_size))
        if abs(new_size - self.cache_size_bytes) > (1024 ** 3):
            self.cache_size_bytes = new_size
            self.clean_cache()

    def load_cache(self):
        self.cached_files = {}
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r") as f:
                    self.cached_files = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                logging.error(f"Failed to load cache config: {e}")

    def save_cache(self):
        try:
            with open(CONFIG_FILE, "w") as f:
                json.dump(self.cached_files, f)
        except IOError as e:
            logging.error(f"Failed to save cache config: {e}")

    def get_cache_size(self):
        try:
            total_size = sum(f.stat().st_size for f in self.cache_dir.glob("**/*") if f.is_file())
            return total_size
        except Exception as e:
            logging.error(f"Error calculating cache size: {e}")
            return 0

    def has_space(self, file_size):
        usage = psutil.disk_usage(self.ssd_path)
        min_free = usage.total * 0.05
        return (usage.free - file_size > min_free) and (self.get_cache_size() + file_size <= self.cache_size_bytes)

    def cache_file(self, file_path):
        file_path = Path(file_path)
        file_str = str(file_path)
        ram_data = self.ram_cache.get(file_str)
        if ram_data is not None:
            logging.info(f"File served from RAM cache: {file_path}")
            return

        if file_str in self.cached_files:
            cache_dest = Path(self.cached_files[file_str])
            with open(cache_dest, 'rb') as f:
                data = f.read()
                if self.ram_cache.add(file_str, data):
                    logging.info(f"Loaded to RAM cache: {file_path}")
            return

        cache_dest = self.cache_dir / file_path.relative_to(self.hdd_dir)
        file_size = file_path.stat().st_size
        self.adjust_cache_size()
        if not self.has_space(file_size):
            self.clean_cache()
            if not self.has_space(file_size):
                return

        try:
            with open(file_path, 'rb') as f:
                data = f.read()
            if self.ram_cache.add(file_str, data):
                logging.info(f"Cached to RAM: {file_path}")
            cache_dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(file_path, cache_dest)
            os.remove(file_path)
            os.symlink(cache_dest, file_path)
            self.cached_files[file_str] = str(cache_dest)
            self.save_cache()
            logging.info(f"Cached to SSD: {file_path}")
        except (IOError, OSError) as e:
            logging.error(f"Failed to cache {file_path}: {e}")
            if cache_dest.exists():
                shutil.move(cache_dest, file_path)

    def clean_cache(self):
        self.ram_cache.trim_cache()
        current_size = self.get_cache_size()
        while current_size > self.cache_size_bytes and self.cached_files:
            try:
                oldest_file = min(self.cached_files.keys(), key=lambda f: Path(f).stat().st_atime)
                cache_dest = Path(self.cached_files[oldest_file])
                os.unlink(oldest_file)
                shutil.move(cache_dest, oldest_file)
                del self.cached_files[oldest_file]
                self.save_cache()
                current_size = self.get_cache_size()
            except (OSError, IOError) as e:
                logging.error(f"Error cleaning cache: {e}")
                del self.cached_files[oldest_file]

    def monitor_system(self):
        while self.running:
            self.adjust_cache_size()
            self.ram_cache.update_max_size()
            time.sleep(60)

    def run(self):
        logging.info("GCache started")
        # Start monitoring thread (non-daemon to keep process alive)
        monitor_thread = threading.Thread(target=self.monitor_system)
        monitor_thread.start()

        # Initial caching run
        for root, _, files in os.walk(self.hdd_dir):
            if not self.running:
                break
            for file in files:
                file_path = Path(root) / file
                if file_path.suffix.lower() in TARGET_EXTENSIONS:
                    self.cache_file(file_path)
        self.clean_cache()
        logging.info("Initial cache run completed")

        # Keep process alive
        while self.running:
            time.sleep(10)  # Check periodically, keeps main thread alive

if __name__ == "__main__":
    try:
        cache_manager = AutoCacheManager()
        cache_manager.run()
    except Exception as e:
        logging.critical(f"GCache failed: {e}")
        # Keep process alive even on error for visibility
        while True:
            time.sleep(10)
