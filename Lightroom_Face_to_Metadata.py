# This script is designed to synchronize face data from Adobe Lightroom Classic to image metadata.
# It reads face regions from the Lightroom catalog and writes them to image metadata using ExifTool.
# The script supports both embedded metadata and sidecar files, and can run in dry-run mode to preview changes.
# Specifically it checks and writes missing Face names based on the MWG standard in the following fields:
#  - Keywords (if IPCT record is present)
#  - Subject (XMP-dc:Subject) 
#  - HierarchicalSubject (XMP-lr:HierarchicalSubject)
#  - PersonInImage (XMP-iptcExt:PersonInImage)
# Based on the database schema derived from: Lightroom Classic 14.3
# The script is needed as Lightroom does not write face data to metadata nor it provides a way to export it via Plugin API.
# Full multi-threaded implementation with error handling and logging; it uses a persistent ExifTool process for efficiency.
# It can be run in dry-run mode to preview changes without writing to files.
#
# The author is not responsible for any damage or loss of data that may occur as a result of using this script.
# Use it at your own risk.
#
# Copyright (c) 2025, Massimo Savazzi
# All rights reserved.
# This script is licensed under the MIT License.
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal 
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
# This script is intended for educational purposes only. Use at your own risk.  
# It is recommended to backup your Lightroom catalog and images before running this script.
# This script is provided "as is" without warranty of any kind, either expressed or implied.    


import os       
import sqlite3
import json
import uuid
import argparse
import logging
import subprocess
import threading
import concurrent.futures
import time
from threading import Lock
from tqdm import tqdm
from typing import List, Tuple, Dict, Optional
from collections import defaultdict
from contextlib import contextmanager

# Image formats that are considered RAW and may require sidecar files for metadata
RAW_FORMATS = {'cr2', 'cr3', 'nef', 'arw', 'rw2', 'orf', 'raf', 'dng', 'pef', 'sr2'}

thread_local = threading.local()
db_pool = None
thread_logger = None


# Database connection pool to manage SQLite connections
class DatabasePool:
    def __init__(self, catalog_path: str ):
        self.catalog_path = catalog_path
        self.connections = []

    # Create a new connection pool for the given catalog path  
    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.catalog_path)
        conn.execute("PRAGMA temp_store = MEMORY")
        conn.execute("PRAGMA mmap_size = 268435456")
        conn.execute("PRAGMA cache_size = 10000")
        conn.execute("PRAGMA synchronous = OFF")
        conn.execute("PRAGMA journal_mode = WAL")
        try:
            yield conn
        finally:
            conn.close()
    
    def close_all(self):
        pass  # Connections are closed automatically by the context manager


# Custom logging filter to add task name to log records
class TaskNameFilter(logging.Filter):
    def filter(self, record):
        # Only add brackets if taskname is not empty
        taskname = getattr(record, 'taskname', '')
        if taskname:
            record.taskname = f'[{taskname}]\t'
        else:
            record.taskname = ''
        return True

# Thread-safe logger to ensure that log messages from multiple threads do not interleave
class ThreadSafeLogger:
    def __init__(self):
        self._lock = Lock()
        self._logger = logging.getLogger()
    
    def _log_with_thread_id(self, level, msg, *args, **kwargs):
        with self._lock:
            self._logger.log(level, msg, *args, **kwargs)
    
    def debug(self, msg, *args, **kwargs):
        self._log_with_thread_id(logging.DEBUG, msg, *args, **kwargs)
    
    def info(self, msg, *args, **kwargs):
        self._log_with_thread_id(logging.INFO, msg, *args, **kwargs)
    
    def warning(self, msg, *args, **kwargs):
        self._log_with_thread_id(logging.WARNING, msg, *args, **kwargs)
    
    def error(self, msg, *args, **kwargs):
        self._log_with_thread_id(logging.ERROR, msg, *args, **kwargs)
    
    def critical(self, msg, *args, **kwargs):
        self._log_with_thread_id(logging.CRITICAL, msg, *args, **kwargs)
    
    def isEnabledFor(self, level):
        return self._logger.isEnabledFor(level)


# ExifTool process management to handle persistent ExifTool instances
class ExifToolProcess:
    
    def __init__(self, exiftool_path: str, thread_id: str):
        self.exiftool_path = exiftool_path
        self.process = None
        self.command_counter = 0
        self.thread_id = thread_id
        
    # Start the ExifTool process in stay-open mode
    def start_process(self):
        if self.process is None or self.process.poll() is not None:
            try:
                self.process = subprocess.Popen(
                    [self.exiftool_path, '-stay_open', 'True', '-@', '-'],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=0
                )
                if thread_logger.isEnabledFor(logging.DEBUG):
                    thread_logger.debug(f"Started ExifTool process PID:\t{self.process.pid}", extra={'taskname': os.path.basename(self.thread_id)})
            except Exception as e:
                thread_logger.error(f"start_process\tFailed to start ExifTool process:\t{e}", extra={'taskname': os.path.basename(self.thread_id)})
                raise

    # Execute a command in the persistent ExifTool process
    def execute_command(self, args: List[str], timeout: int = 30) -> Tuple[str, str, int]:
        if self.process is None or self.process.poll() is not None:
            self.start_process()
        
        try:
            # Prepare command
            ready_token = "{ready}"
            command = '\n'.join(args) + f'\n-execute\n'
            
            if thread_logger.isEnabledFor(logging.DEBUG):
                thread_logger.debug(f"ExifTool command:\t{' '.join(args)}", extra={'taskname': os.path.basename(self.thread_id)})
            
            # Send command
            self.process.stdin.write(command)
            self.process.stdin.flush()
            
            # retrieve output in a compatible way for Winows and Unix
            output = ""
            while ready_token not in output:
                chunk = self.process.stdout.read(1)
                if not chunk:
                    break
                output += chunk

            # Remove the ready token from output
            output = output.replace(ready_token, "")
            stdout = output.strip()

            # impossible to retrieve stderr in stay_open mode on Windows, so we assume no errors
            stderr = ""
            
            return stdout, stderr, 0
            
        except Exception as e:
            thread_logger.error(f"execute_command\tExifTool command execution failed:\t{e}", extra={'taskname': os.path.basename(self.thread_id)})
            return "", str(e), 1
    
    # Close the ExifTool process gracefully
    def close(self):
        if self.process and self.process.poll() is None:
            try:
                self.process.stdin.write('-stay_open\nFalse\n')
                self.process.stdin.flush()
                self.process.wait(timeout=5)
                if thread_logger.isEnabledFor(logging.DEBUG):
                    thread_logger.debug(f"Closed ExifTool process PID:\t{self.process.pid}", extra={'taskname': os.path.basename(self.thread_id)})
            except:
                self.process.terminate()
                self.process.wait(timeout=2)
            finally:
                self.process = None

# Function to get or create a thread-local ExifTool process
def get_exiftool_process(exiftool_path: str, thread_id:str) -> ExifToolProcess:
    if not hasattr(thread_local, 'exiftool_process'):
        thread_local.exiftool_process = ExifToolProcess(exiftool_path, thread_id)
        thread_local.exiftool_process.start_process()
    return thread_local.exiftool_process

# Function to clean up the thread-local ExifTool process
def cleanup_exiftool_process():
    if hasattr(thread_local, 'exiftool_process'):
        thread_local.exiftool_process.close()
        delattr(thread_local, 'exiftool_process')

# Function to parse command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="Sync Lightroom face data to image metadata.")
    parser.add_argument('--catalog', required=True, help='Path to Lightroom .lrcat file')
    parser.add_argument('--log', default='log.txt', help='Path to save the log file')
    parser.add_argument('--write', action='store_true', help='Enable actual writing (default is dry-run)')
    parser.add_argument('--exiftool-path', default='exiftool', help='Path to the exiftool executable')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set the logging level')
    parser.add_argument('--tags', action='store_true', help='Write tags to metadata (Keywords, Subject, HierarchicalSubject, PersonInImage)')
    parser.add_argument('--profile', nargs='?', const='profile.prof', default=None, help='Enable profiling and save output to specified file (default: profile.prof)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Database query batch size (default: 1000)')
    parser.add_argument('--threads', type=int, default=0, help='Number of threads for file operations (0 = auto-detect optimal)')
    parser.add_argument('--max-threads', type=int, default=16, help='Maximum number of threads to use when auto-detecting (default: 16)')   
    return parser.parse_args()

# Initialize logging with a thread-safe logger
def init_logging(log_path: str, session_id: str, log_level: str):
    global thread_logger
    formatter = logging.Formatter(
        fmt='%(asctime)s\t[Session {session_id}]\t[%(threadName)s]\t%(taskname)s\t%(levelname)s\t%(message)s'.format(session_id=session_id),
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler = logging.FileHandler(log_path, encoding='utf-8')
    handler.setFormatter(formatter)
    handler.addFilter(TaskNameFilter())
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.handlers.clear()
    logger.addHandler(handler)

    thread_logger = ThreadSafeLogger()

# Function to fetch keyword hierarchy from the Lightroom catalog in batches
def fetch_keyword_hierarchy(catalog_path: str, batch_size: int = 1000) -> Dict[int, str]:
    global db_pool
    hierarchy = {}
    total_processed = 0
    try:
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM AgLibraryKeyword WHERE name IS NOT NULL")
            total_keywords = cursor.fetchone()[0]
            if thread_logger.isEnabledFor(logging.INFO):
                thread_logger.info(f"Total keywords to process:\t{total_keywords}")
            # Recursive query to build the keyword hierarchy
            cursor.execute("""
                WITH RECURSIVE path_builder(id_local, name, parent, full_path, level) AS (
                    SELECT 
                        id_local,
                        COALESCE(name, '') as name,
                        parent,
                        COALESCE(name, '') AS full_path,
                        0 as level
                    FROM AgLibraryKeyword
                    WHERE parent IS NULL
                    UNION ALL
                    SELECT
                        LK.id_local,
                        LK.name,
                        LK.parent,
                        CASE
                            WHEN pb.full_path = '' THEN LK.name
                            ELSE pb.full_path || '|' || LK.name
                        END AS full_path,
                        pb.level + 1
                    FROM AgLibraryKeyword LK
                    INNER JOIN path_builder pb ON LK.parent = pb.id_local
                    WHERE LK.name IS NOT NULL AND pb.level < 10
                )
                SELECT id_local, full_path
                FROM path_builder
                WHERE full_path != '' AND level > 0
                ORDER BY level, full_path;
            """)
            # Fetch the keyword hierarchy in batches
            batch_count = 0
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                batch_count += 1
                batch_processed = 0
                for id_local, full_path in batch:
                    hierarchy[id_local] = full_path
                    batch_processed += 1
                    total_processed += 1
                    if thread_logger.isEnabledFor(logging.DEBUG):
                        thread_logger.debug(f"Keyword hierarchy:\t{id_local}\t->\t{full_path}")
                if thread_logger.isEnabledFor(logging.INFO):
                    thread_logger.info(f"Batch {batch_count}:\tprocessed {batch_processed}\tkeywords (total: {total_processed})")
                if batch_count % 10 == 0:
                    import gc
                    gc.collect()
            if thread_logger.isEnabledFor(logging.INFO):
                thread_logger.info(f"Completed:\t{total_processed} keyword hierarchies loaded")
    except sqlite3.Error as e:
        thread_logger.error(f"fetch_keyword_hierarchy\tDatabase error:\t{e}")
        return {}
    except Exception as e:
        thread_logger.error(f"fetch_keyword_hierarchy\tUnexpected error:\t{e}")
        return {}
    return hierarchy

# Function to fetch face data from the Lightroom catalog in batches
def fetch_face_data_batch(catalog_path: str, batch_size: int = 1000) -> List[Tuple]:
    global db_pool
    results = []
    total_processed = 0
    try:
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            # Count total face regions to process
            cursor.execute("""
                SELECT COUNT(*)
                FROM AgLibraryKeyword LK
                INNER JOIN AgLibraryKeywordFace LKF ON LK.id_local = LKF.tag
                INNER JOIN AgLibraryFace LF ON LKF.face = LF.id_local 
                WHERE LK.name IS NOT NULL
                    AND LF.tl_x IS NOT NULL 
                    AND LF.tl_y IS NOT NULL
                    AND LF.br_x IS NOT NULL 
                    AND LF.br_y IS NOT NULL
            """)
            total_faces = cursor.fetchone()[0]
            if thread_logger.isEnabledFor(logging.INFO):
                thread_logger.info(f"Total face regions to process:\t{total_faces}")
            # Query to fetch face regions with their associated keywords
            cursor.execute("""
                SELECT
                    LRF.absolutePath as rootFile,
                    LFi.baseName || '.' || LFi.extension as fileName, 
                    LFo.pathFromRoot,
                    LFi.extension,
                    LK.name,
                    LK.id_local,
                    LF.tl_x as face_left,
                    LF.tl_y as face_top, 
                    LF.br_x as face_right, 
                    LF.br_y as face_bottom,
                    (LF.br_x - LF.tl_x) AS face_width,
                    (LF.br_y - LF.tl_y) AS face_height,
                    (LF.tl_x + (LF.br_x - LF.tl_x) * 0.5) AS face_center_x,
                    (LF.tl_y + (LF.br_y - LF.tl_y) * 0.5) AS face_center_y
                FROM AgLibraryKeyword LK
                INNER JOIN AgLibraryKeywordFace LKF ON LK.id_local = LKF.tag
                INNER JOIN AgLibraryFace LF ON LKF.face = LF.id_local 
                INNER JOIN Adobe_images AI ON AI.id_local = LF.image
                INNER JOIN AgLibraryFile LFi ON AI.rootFile = LFi.id_local
                INNER JOIN AgLibraryFolder LFo ON LFi.folder = LFo.id_local 
                INNER JOIN AgLibraryRootFolder LRF ON LFo.rootFolder = LRF.id_local
                WHERE LK.name IS NOT NULL
                    AND LF.tl_x IS NOT NULL 
                    AND LF.tl_y IS NOT NULL
                    AND LF.br_x IS NOT NULL 
                    AND LF.br_y IS NOT NULL
                ORDER BY LRF.absolutePath, LFi.baseName, LFi.extension, LK.name;
            """)
            #
            batch_count = 0
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                batch_count += 1
                batch_processed = 0
                for row in batch:
                    (rootPath, fileName, folderPath, ext, name, keyword_id, 
                     left, top, right, bottom, cw, ch, cx, cy) = row
                    full_path = os.path.normpath(os.path.join(rootPath, folderPath, fileName))
                    results.append((full_path, ext, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy))
                    batch_processed += 1
                    total_processed += 1
                    if thread_logger.isEnabledFor(logging.DEBUG):
                        thread_logger.debug(f"DB Face:\t{full_path}\t{ext}\t{name}\t{keyword_id}\t{left}\t{top}\t{right}\t{bottom}\t{cw}\t{ch}\t{cx}\t{cy}")
                if thread_logger.isEnabledFor(logging.INFO):
                    thread_logger.info(f"Batch {batch_count}:\tprocessed {batch_processed}\tface regions (total: {total_processed})")
                if batch_count % 10 == 0:
                    import gc
                    gc.collect()
            if thread_logger.isEnabledFor(logging.INFO):
                thread_logger.info(f"Completed:\t{total_processed} face regions loaded")
    except sqlite3.Error as e:
        thread_logger.error(f"fetch_face_data_batch\tDatabase error:\t{e}")
        return []
    except Exception as e:
        thread_logger.error(f"fetch_face_data_batch\tUnexpected error:\t{e}")
        return []
    return results

# Function to check if a face region is a duplicate based on name or coordinates
def is_duplicate(existing, name, cw, ch, cx, cy):
    for e_name, ex, ey, ew, eh in existing:
        if name == e_name:
            return 'name'
        if (cx - ex) ** 2 + (cy - ey) ** 2 < 0.0004:
            return 'area'
    return None

# Function to check if an image has IPTC data
def has_iptc_data(image_path, exiftool_path):
    try:
        exiftool = get_exiftool_process(exiftool_path,os.path.basename(image_path))
        args = ['-b', '-IPTC:all', image_path]
        
        stdout, stderr, returncode = exiftool.execute_command(args, timeout=10)
        
        if returncode != 0:
            thread_logger.warning(f"has_iptc_data\tExifTool warning on {image_path}: {stderr}")
            return False
            
        # If there is any output, IPTC data exists
        return bool(stdout.strip())
        
    except Exception as e:
        thread_logger.error(f"has_iptc_data\tError checking IPTC data for {image_path}: {e}")
        return False

# Function to extract existing metadata from an image file using ExifTool
def extract_existing_metadata(file_path, exiftool_path,  is_sidecar, iptc_available) -> Tuple[List[Tuple[str, float, float, float, float]], List[str], List[str], List[str], List[str]]:
    # Function to read all metadata from an image file using ExifTool
    def read_all_metadata(target_path):
        try:
            exiftool = get_exiftool_process(exiftool_path,os.path.basename(file_path))
            
            args = [
                '-j', '-struct', '-fast',
                *(['-Keywords'] if (not is_sidecar and iptc_available) else []),
                '-Subject', '-HierarchicalSubject',
                '-RegionName', '-RegionType', '-RegionAreaX', '-RegionAreaY', 
                '-RegionAreaW', '-RegionAreaH', '-RegionInfo',
                '-PersonInImage',
                target_path
            ]
            if thread_logger.isEnabledFor(logging.DEBUG):
                thread_logger.debug(f"Read all metadata\t{' '.join(args)}", extra={'taskname': os.path.basename(file_path)})

            stdout, stderr, returncode = exiftool.execute_command(args, timeout=10)
            
            if returncode != 0:
                thread_logger.warning(f"read_all_metadata\tExifTool warning on {target_path}: {stderr.strip()}", extra={'taskname': os.path.basename(file_path)})
                return [], [], [], [], [] 
                
            if not stdout.strip():
                thread_logger.warning(f"read_all_metadata\tExifTool empty output on {target_path}: {stderr.strip()}", extra={'taskname': os.path.basename(file_path)})
                return [], [], [], [], []
                
            data_list = json.loads(stdout)

            if not data_list:
                thread_logger.warning(f"read_all_metadata\tExifTool JSON load failed on\t{target_path}:\t{result.stderr.strip()}", extra={'taskname': os.path.basename(file_path)})
                return [], [], [], [], []
            data = data_list[0]
            existing_faces = extract_faces_from_data(data)
            keywords = normalize_to_list(data.get('Keywords')) if (not is_sidecar and iptc_available) else []
            subject = normalize_to_list(data.get('Subject'))
            hierarchical = normalize_to_list(data.get('HierarchicalSubject'))
            persons_in_image = normalize_to_list(data.get('PersonInImage'))
            return existing_faces, keywords, subject, hierarchical, persons_in_image

        except json.JSONDecodeError as e:
            thread_logger.error(f"read_all_metadata\tJSON decode error for\t{target_path}:\t{e}", extra={'taskname': os.path.basename(file_path)})
            return [], [], [], [], []
        except Exception as e:
            thread_logger.error(f"read_all_metadata\tUnexpected error reading\t{target_path}:\t{str(e)}", extra={'taskname': os.path.basename(file_path)})
            return [], [], [], [], []
        
    # Function to normalize a value to a list
    def normalize_to_list(value):
        if value is None:
            return []
        elif isinstance(value, list):
            return value
        elif isinstance(value, str):
            return [value]
        else:
            return []
        
    if is_sidecar:
        sidecar = os.path.splitext(file_path)[0] + ".xmp"
        if os.path.isfile(sidecar):
            if thread_logger.isEnabledFor(logging.INFO):
                thread_logger.info(f"Reading XMP sidecar for\t{file_path}", extra={'taskname': os.path.basename(file_path)})
            file_path = sidecar
        else:
            thread_logger.warning(f"XMP sidecar does not exist for\t{sidecar}", extra={'taskname': os.path.basename(file_path)})

    faces, keywords, subject, hierarchical, persons_in_image = read_all_metadata(file_path)
    if faces or keywords or subject or hierarchical or persons_in_image:
        return faces, keywords, subject, hierarchical, persons_in_image
        
    return [], [], [], [], []

# Function to extract face regions from metadata JSON data
def extract_faces_from_data(data) -> List[Tuple[str, float, float, float, float]]:
    existing_faces = []
    def safe_float_convert(value):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    names = data.get('RegionName')
    types = data.get('RegionType')
    coords = [data.get(key) for key in ['RegionAreaX', 'RegionAreaY', 'RegionAreaW', 'RegionAreaH']]
    if names and types and all(coord is not None for coord in coords):
        xs, ys, ws, hs = coords
        for arr in [names, types, xs, ys, ws, hs]:
            if not isinstance(arr, list):
                arr = [arr]
        min_len = min(len(arr) if isinstance(arr, list) else 1 
                     for arr in [names, types, xs, ys, ws, hs])
        for i in range(min_len):
            name = names[i] if isinstance(names, list) else names
            type_ = types[i] if isinstance(types, list) else types
            x = safe_float_convert(xs[i] if isinstance(xs, list) else xs)
            y = safe_float_convert(ys[i] if isinstance(ys, list) else ys)
            w = safe_float_convert(ws[i] if isinstance(ws, list) else ws)
            h = safe_float_convert(hs[i] if isinstance(hs, list) else hs)
            if type_ == 'Face' and all(v is not None for v in [name, x, y, w, h]):
                existing_faces.append((name, x, y, w, h))
        return existing_faces
    region_info = data.get('RegionInfo')
    if isinstance(region_info, dict):
        region_list = region_info.get('RegionList')
        if isinstance(region_list, list):
            for reg in region_list:
                if not isinstance(reg, dict):
                    continue
                name = reg.get('RegionName') or reg.get('Name')
                type_ = reg.get('RegionType') or reg.get('Type')
                if type_ != 'Face' or not name:
                    continue
                coords = [reg.get(key) for key in ['RegionAreaX', 'RegionAreaY', 'RegionAreaW', 'RegionAreaH']]
                if any(coord is None for coord in coords):
                    area = reg.get('Area')
                    if isinstance(area, dict):
                        coords = [area.get(key) for key in ['X', 'Y', 'W', 'H']]
                x, y, w, h = [safe_float_convert(coord) for coord in coords]
                if all(v is not None for v in [x, y, w, h]):
                    existing_faces.append((name, x, y, w, h))
    return existing_faces

# Function to write metadata in batch using ExifTool
def write_metadata_batch(image_path, face_regions: List[Tuple], keywords_to_add: Dict[str, List[str]], 
                        dry_run: bool, exiftool_path: str, use_sidecar: bool, iptc_available: bool) -> bool:
    if not face_regions and not any(keywords_to_add.values()):
        if thread_logger.isEnabledFor(logging.DEBUG):
            thread_logger.debug(f"write_metadata_batch\tskipping write for\t{image_path}\t no face regions or keywords to add", extra={'taskname': os.path.basename(image_path)})
        return True
    try:
        exiftool = get_exiftool_process(exiftool_path,os.path.basename(image_path))
        
        args = ['-overwrite_original', '-fast']
        target = image_path
        if use_sidecar:
            sidecar_path = os.path.splitext(image_path)[0] + ".xmp"
            args.extend(['-use', 'MWG', '-tagsFromFile', '@', '-all:all'])
            target = sidecar_path
        if face_regions:
            field_prefix = '-XMP-mwg-rs:' if use_sidecar else '-'
            for name, x, y, w, h in face_regions:
                args.extend([
                    f'{field_prefix}RegionName+={name}',
                    f'{field_prefix}RegionType+=Face',
                    f'{field_prefix}RegionAreaX+={x}',
                    f'{field_prefix}RegionAreaY+={y}',
                    f'{field_prefix}RegionAreaW+={w}',
                    f'{field_prefix}RegionAreaH+={h}'
                ])

        # As per best practice on ExifTool documentation, we use the the simplest form of the tag name
        # and we add the prefix only if the tag is in a sidecar file as they can have only XMP tags
        # Only write keywords if not a sidecar file
        if use_sidecar:
            keyword_mappings = {
                'subject': '-XMP-dc:Subject',
                'hierarchical': '-XMP-lr:HierarchicalSubject',
                'persons_in_image': '-XMP-iptcExt:PersonInImage'
            }
        else:
            # If IPTC data is available, write keywords
            if iptc_available:
                keyword_mappings = {
                    'keywords': '-Keywords',
                    'subject': '-Subject',
                    'hierarchical': '-HierarchicalSubject',
                    'persons_in_image': '-PersonInImage'
                }
            else:
                keyword_mappings = {
                    'subject': '-Subject',
                    'hierarchical': '-HierarchicalSubject',
                    'persons_in_image': '-PersonInImage'
                }

        for field, prefix in keyword_mappings.items():
            for keyword in keywords_to_add.get(field, []):
                args.append(f'{prefix}+={keyword}')

        args.append(target)
        if thread_logger.isEnabledFor(logging.DEBUG):
            thread_logger.debug(f"write_metadata_batch\tBatch write:\t{' '.join(args)}", extra={'taskname': os.path.basename(image_path)})
        if not dry_run:
            stdout, stderr, returncode = exiftool.execute_command(args, timeout=30)
            
            if returncode != 0:
                thread_logger.error(f"write_metadata_batch\tExifTool error writing to {target}: {stderr.strip()}", extra={'taskname': os.path.basename(image_path)})
                return False
            else:
                if thread_logger.isEnabledFor(logging.INFO):
                    thread_logger.info(f"Wrote metadata to {target}", extra={'taskname': os.path.basename(image_path)})
                return True
        
        return True
        
    except Exception as e:
        thread_logger.error(f"write_metadata_batch\tError writing to {image_path}: {str(e)}", extra={'taskname': os.path.basename(image_path)})
        return False

# Function to process a single file and write face regions and keywords to metadata
def process_file_keywords(full_path, face_list, args, keyword_hierarchy):
    try:
        if not os.path.isfile(full_path):
            thread_logger.error(f"process_file_keywords\tFile not found: {full_path}", extra={'taskname': os.path.basename(full_path)})
            return False
        fmt = face_list[0][0]

        use_sidecar = fmt.lower() in RAW_FORMATS
        log_type = "sidecar" if use_sidecar else "embedded"

        iptc_available = False
        if not use_sidecar:
            iptc_available = has_iptc_data(full_path, args.exiftool_path)
            if thread_logger.isEnabledFor(logging.INFO):
                thread_logger.info(f"IPTC data:\t{'found' if iptc_available else 'not found'}", extra={'taskname': os.path.basename(full_path)})


        existing_faces, existing_keywords, existing_subject, existing_hierarchical, existing_persons_in_image = extract_existing_metadata(
            full_path, args.exiftool_path, use_sidecar, iptc_available
        )
        new_face_regions = []
        keywords_to_add = {
            'keywords': [],
            'subject': [], 
            'hierarchical': [],
            'persons_in_image': []
        }
        existing_keywords_set = set(existing_keywords)
        existing_subject_set = set(existing_subject)
        existing_hierarchical_set = set(existing_hierarchical)
        existing_persons_set = set(existing_persons_in_image)
        # Add existing faces to a set for quick lookup
        for fmt, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy in face_list:
            if thread_logger.isEnabledFor(logging.WARNING):   
                thread_logger.warning(f"Processing {name}", extra={'taskname': os.path.basename(full_path)})
            dup_type = is_duplicate(existing_faces, name, cw, ch, cx, cy)
            if not dup_type:
                new_face_regions.append((name, cx, cy, cw, ch))
                if thread_logger.isEnabledFor(logging.INFO):
                    thread_logger.info(f"Queued Face '{name}'", extra={'taskname': os.path.basename(full_path)})
            else:
                if thread_logger.isEnabledFor(logging.INFO):
                    thread_logger.info(f"Duplicate ({dup_type}): {name}", extra={'taskname': os.path.basename(full_path)})
            # Check if the keyword_id is in the hierarchy and add keywords accordingly
            if args.tags and keyword_id in keyword_hierarchy:
                hierarchical_keyword = keyword_hierarchy[keyword_id]
                simple_keyword = hierarchical_keyword.split('|')[-1] if '|' in hierarchical_keyword else hierarchical_keyword
                if (not use_sidecar and iptc_available) and simple_keyword not in existing_keywords_set:
                    keywords_to_add['keywords'].append(simple_keyword)
                    existing_keywords_set.add(simple_keyword)
                    if thread_logger.isEnabledFor(logging.INFO):
                        thread_logger.info(f"Queued Keywords field '{simple_keyword}'", extra={'taskname': os.path.basename(full_path)})
                if simple_keyword not in existing_subject_set:
                    keywords_to_add['subject'].append(simple_keyword)
                    existing_subject_set.add(simple_keyword)
                    if thread_logger.isEnabledFor(logging.INFO):
                        thread_logger.info(f"Queued Subject field '{simple_keyword}'", extra={'taskname': os.path.basename(full_path)})
                if simple_keyword not in existing_persons_set:
                    keywords_to_add['persons_in_image'].append(simple_keyword)
                    existing_persons_set.add(simple_keyword)
                    if thread_logger.isEnabledFor(logging.INFO):
                        thread_logger.info(f"Queued PersonsInImage field '{simple_keyword}'", extra={'taskname': os.path.basename(full_path)})
                if hierarchical_keyword not in existing_hierarchical_set:
                    keywords_to_add['hierarchical'].append(hierarchical_keyword)
                    existing_hierarchical_set.add(hierarchical_keyword)
                    if thread_logger.isEnabledFor(logging.INFO):
                        thread_logger.info(f"Queued HierarchicalSubject field '{hierarchical_keyword}'", extra={'taskname': os.path.basename(full_path)})
        total_keywords = sum(len(v) for v in keywords_to_add.values())
        # If no new face regions or keywords to add, skip writing
        if new_face_regions or total_keywords > 0:
            success = write_metadata_batch(
                full_path, new_face_regions, keywords_to_add,
                dry_run=not args.write, exiftool_path=args.exiftool_path,
                use_sidecar=use_sidecar, iptc_available=iptc_available
            )
            if success:
                action = "Wrote" if args.write else "Simulated write"
                face_count = len(new_face_regions)
                if thread_logger and thread_logger.isEnabledFor(logging.WARNING):
                    thread_logger.warning(f"{action} ({log_type}) batch: {face_count} faces, {total_keywords} total keywords to {full_path}", extra={'taskname': os.path.basename(full_path)})
            return success
        return True
    except Exception as e:
        thread_logger.error(f"process_file_keywords\tError processing {full_path}: {str(e)}", extra={'taskname': os.path.basename(full_path)})
        return False
    finally:
        # Clean up ExifTool process when thread is done
        cleanup_exiftool_process()

def main():
    global db_pool
    args = parse_args()
    session_id = uuid.uuid4().hex[:8]
    init_logging(args.log, session_id, args.log_level)
    if thread_logger.isEnabledFor(logging.WARNING):
        thread_logger.warning(f'Session\t{session_id}\tstarted')
    db_pool = DatabasePool(args.catalog)

    # --- Parallel DB queries ---
    keyword_hierarchy = {}
    face_data = []

    # Define functions to load keywords and face data in parallel
    def load_keywords():
        nonlocal keyword_hierarchy
        start = time.time()
        if args.tags:
            keyword_hierarchy = fetch_keyword_hierarchy(args.catalog, args.batch_size)
            if thread_logger.isEnabledFor(logging.WARNING):
                thread_logger.warning(f'Loaded\t{len(keyword_hierarchy)} keyword hierarchies')
        elapsed = time.time() - start
        if thread_logger.isEnabledFor(logging.WARNING):
            thread_logger.warning(f"Keyword hierarchy loading took\t{elapsed:.2f} seconds")

    # Define function to load face data in parallel
    def load_faces():
        nonlocal face_data
        start = time.time()
        face_data = fetch_face_data_batch(args.catalog, args.batch_size)
        if thread_logger.isEnabledFor(logging.WARNING):
            thread_logger.warning(f'face regions:\t{len(face_data)}\tfound in\t{args.catalog}')
        elapsed = time.time() - start
        if thread_logger.isEnabledFor(logging.WARNING):
            thread_logger.warning(f"Face Data loading took\t{elapsed:.2f} seconds")
    
    # Start threads to load keywords and face data
    t1 = threading.Thread(target=load_keywords, name="DB-Keyword-Thread")
    t2 = threading.Thread(target=load_faces, name="DB-Face-Thread")
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    db_pool.close_all()
    
    # Check if any face data was loaded
    if not face_data:
        thread_logger.critical("No face data found in catalog")
        return

    # Prepare data for processing
    file_data = defaultdict(list)
    for row in face_data:
        full_path, fmt, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy = row
        file_data[full_path].append((fmt, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy))

    if thread_logger.isEnabledFor(logging.WARNING):
        thread_logger.warning(f'Processing {len(file_data)} unique image files')

    # Determine number of threads to use
    cpu_count = os.cpu_count() or 4
    max_threads = args.max_threads
    num_threads = args.threads if args.threads > 0 else min(cpu_count * 2, max_threads)

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads, thread_name_prefix="FileWorker") as executor:
        futures = {}
        for full_path, face_list in tqdm(file_data.items(), desc="Processing images", unit="files"):
            future = executor.submit(process_file_keywords, full_path, face_list, args, keyword_hierarchy)
            futures[future] = full_path
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Waiting for threads"):
            full_path = futures[future]
            try:
                result = future.result()
                results[full_path] = result
            except Exception as e:
                thread_logger.error(f"Error processing {full_path}: {e}")
                results[full_path] = False
            finally:
                # Ensure cleanup happens even if there's an exception
                try:
                    cleanup_exiftool_process()
                except:
                    pass  # Ignore cleanup errors

    successful = sum(1 for v in results.values() if v)
    total = len(results)
    if thread_logger.isEnabledFor(logging.WARNING):
        thread_logger.warning(f"Image processing completed: {successful}/{total} successful")
    thread_logger.warning(f"Session {session_id} complete")

if __name__ == '__main__':
    main()