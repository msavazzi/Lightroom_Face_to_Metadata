# Script to sync Lightroom face data to image metadata using ExifTool
# Based on the database schema from: Lightroom Classic 14.3
# Needed as Lightroom does not write face data to metadata nor it provides a way to export it via Plugin API
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
# The author is not responsible for any damage or loss of data that may occur as a result of using this script.

import os       
import sqlite3
import json
import uuid
import argparse
import logging
import subprocess
import cProfile
import pstats
import io
from tqdm import tqdm
from datetime import datetime
from typing import List, Tuple, Dict, Optional
from collections import defaultdict
from contextlib import contextmanager

# RAW formats that typically require XMP sidecars
RAW_FORMATS = {'cr2', 'cr3', 'nef', 'arw', 'rw2', 'orf', 'raf', 'dng', 'pef', 'sr2'}

# Database connection pool for reuse
class DatabasePool:
    def __init__(self, catalog_path: str, max_connections: int = 5):
        self.catalog_path = catalog_path
        self.connections = []
        self.max_connections = max_connections
        
    @contextmanager
    def get_connection(self):
        if self.connections:
            conn = self.connections.pop()
        else:
            conn = sqlite3.connect(self.catalog_path)
            # Optimize SQLite settings for read operations
            conn.execute("PRAGMA temp_store = MEMORY")
            conn.execute("PRAGMA mmap_size = 268435456")  # 256MB
            conn.execute("PRAGMA cache_size = 10000")
            conn.execute("PRAGMA synchronous = OFF")
            conn.execute("PRAGMA journal_mode = WAL")
        
        try:
            yield conn
        finally:
            if len(self.connections) < self.max_connections:
                self.connections.append(conn)
            else:
                conn.close()
    
    def close_all(self):
        for conn in self.connections:
            conn.close()
        self.connections.clear()

# Global database pool instance
db_pool = None

# Function to parse command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="Sync Lightroom face data to image metadata.")
    parser.add_argument('--catalog', required=True, help='Path to Lightroom .lrcat file')
    parser.add_argument('--log', default='log.txt', help='Path to save the log file')
    parser.add_argument('--write', action='store_true', help='Enable actual writing (default is dry-run)')
    parser.add_argument('--exiftool-path', default='exiftool', help='Path to the exiftool executable')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set the logging level')
    parser.add_argument('--write-hierarchical-tags', action='store_true', help='Write hierarchical keyword tags to metadata')
    parser.add_argument('--profile', nargs='?', const='profile.prof', default=None, help='Enable profiling and save output to specified file (default: profile.prof)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Database query batch size (default: 1000)')
    return parser.parse_args()

# Function to initialize logging
def init_logging(log_path: str, session_id: str, log_level: str):
    logging.basicConfig(
        filename=log_path,
        level=getattr(logging, log_level.upper(), logging.INFO),
        format=f'%(asctime)s\t[Session {session_id}]\t%(levelname)s:\t%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

#    Fetch keyword hierarchy with batch processing for memory efficiency.
#    Uses recursive CTE with batched result processing to handle large keyword sets.
def fetch_keyword_hierarchy(catalog_path: str, batch_size: int = 1000) -> Dict[int, str]:
    global db_pool
    hierarchy = {}
    total_processed = 0
    
    try:
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            
            # First, get total count for progress tracking
            cursor.execute("SELECT COUNT(*) FROM AgLibraryKeyword WHERE name IS NOT NULL")
            total_keywords = cursor.fetchone()[0]
            if logging.getLogger().isEnabledFor(logging.INFO):
                logging.info(f"Total keywords to process:\t{total_keywords}")
            
            # Optimized recursive CTE with better performance hints
            cursor.execute("""
                WITH RECURSIVE path_builder(id_local, name, parent, full_path, level) AS (
                    -- Base case: root nodes (parent IS NULL)
                    SELECT 
                        id_local,
                        COALESCE(name, '') as name,
                        parent,
                        COALESCE(name, '') AS full_path,
                        0 as level
                    FROM AgLibraryKeyword
                    WHERE parent IS NULL
                    
                    UNION ALL
                    
                    -- Recursive case: build full_path by appending child name
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
                    WHERE LK.name IS NOT NULL AND pb.level < 10  -- Prevent infinite recursion
                )
                SELECT id_local, full_path
                FROM path_builder
                WHERE full_path != '' AND level > 0
                ORDER BY level, full_path;
            """)
            
            # Process results in batches to manage memory
            batch_count = 0
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                
                batch_count += 1
                batch_processed = 0
                
                # Build hierarchy dictionary with better memory efficiency
                for id_local, full_path in batch:
                    hierarchy[id_local] = full_path
                    batch_processed += 1
                    total_processed += 1
                    
                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                        logging.debug(f"Keyword hierarchy:\t{id_local}\t->\t{full_path}")
                
                if logging.getLogger().isEnabledFor(logging.INFO):
                    logging.info(f"Batch {batch_count}:\tprocessed {batch_processed}\tkeywords (total: {total_processed})")
                
                # Optional: Force garbage collection for large batches
                if batch_count % 10 == 0:
                    import gc
                    gc.collect()
            
            if logging.getLogger().isEnabledFor(logging.INFO):
                logging.info(f"Completed:\t{total_processed} keyword hierarchies loaded")
            
    except sqlite3.Error as e:
        logging.error(f"fetch_keyword_hierarchy\tDatabase error:\t{e}")
        return {}
    except Exception as e:
        logging.error(f"fetch_keyword_hierarchy\tUnexpected error:\t{e}")
        return {}
    
    return hierarchy

# Optimized face data fetching with proper indexing and batch processing.
def fetch_face_data_batch(catalog_path: str, batch_size: int = 1000) -> List[Tuple]:
    global db_pool
    results = []
    total_processed = 0
    
    try:
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get total count for progress tracking
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
            if logging.getLogger().isEnabledFor(logging.INFO):
                logging.info(f"Total face regions to process:\t{total_faces}")
            
            # Optimized main query with better join order and reduced calculations
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
            
            # Process results in batches to manage memory
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
                    
                    # Normalize path once per row
                    full_path = os.path.normpath(os.path.join(rootPath, folderPath, fileName))
                    
                    results.append((full_path, ext, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy))
                    batch_processed += 1
                    total_processed += 1
                    
                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                        logging.debug(f"DB Face:\t{full_path}\t{ext}\t{name}\t{keyword_id}\t{left}\t{top}\t{right}\t{bottom}\t{cw}\t{ch}\t{cx}\t{cy}")
                
                if logging.getLogger().isEnabledFor(logging.INFO):
                    logging.info(f"Batch {batch_count}:\tprocessed {batch_processed}\tface regions (total: {total_processed})")
                
                # Optional: Force garbage collection for large batches
                if batch_count % 10 == 0:
                    import gc
                    gc.collect()
            
            if logging.getLogger().isEnabledFor(logging.INFO):
                logging.info(f"Completed:\t{total_processed} face regions loaded")
            
    except sqlite3.Error as e:
        logging.error(f"fetch_face_data_batch\tDatabase error:\t{e}")
        return []
    except Exception as e:
        logging.error(f"fetch_face_data_batch\tUnexpected error:\t{e}")
        return []
    
    return results

# Check if a face region is a duplicate based on name or area with optimized comparison
def is_duplicate(existing, name, cw, ch, cx, cy):
    for e_name, ex, ey, ew, eh in existing:
        if name == e_name:
            return 'name'
        # Use squared distance to avoid sqrt calculation
        if (cx - ex) ** 2 + (cy - ey) ** 2 < 0.0004:  # equivalent to 0.02 threshold
            return 'area'
    return None

# Optimized metadata extraction with better error handling and caching.
def extract_existing_metadata(file_path, exiftool_path) -> Tuple[List[Tuple[str, float, float, float, float]], List[str], List[str], List[str]]:

    # Function to read all metadata from a file using ExifTool with optimized command
    def read_all_metadata(target_path):
        # Optimized ExifTool command with specific field selection
        cmd = [
            exiftool_path, '-j', '-struct', '-fast',  # Fast mode for better performance - fast2 could miss some metadata
            '-Keywords', '-Subject', '-HierarchicalSubject',
            '-RegionName', '-RegionType', '-RegionAreaX', '-RegionAreaY', 
            '-RegionAreaW', '-RegionAreaH', '-RegionInfo',
            target_path
        ]
        
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f"Read all metadata\t{' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=10,  # Reduced timeout for faster failure detection
                check=False
            )
            
            if result.returncode != 0:
                logging.warning(f"read_all_metadata\tExifTool warning on\t{target_path}:\t{result.stderr.strip()}")
                return [], [], [], []

            # Check if output is empty or malformed
            if not result.stdout.strip():
                logging.warning(f"read_all_metadata\tExifTool empty or malformed output on\t{target_path}:\t{result.stderr.strip()}")
                return [], [], [], []
                
            data_list = json.loads(result.stdout)
            # Check if data_list is not loaded or is empty
            if not data_list:
                logging.warning(f"read_all_metadata\tExifTool JSON load failed on\t{target_path}:\t{result.stderr.strip()}")
                return [], [], [], []

            data = data_list[0]
            
            # Extract face regions with optimized parsing
            existing_faces = extract_faces_from_data(data)
            
            # Extract and normalize keywords efficiently
            keywords = normalize_to_list(data.get('Keywords'))
            subject = normalize_to_list(data.get('Subject'))
            hierarchical = normalize_to_list(data.get('HierarchicalSubject'))
                
            return existing_faces, keywords, subject, hierarchical
            
        except subprocess.TimeoutExpired:
            logging.warning(f"read_all_metadata\tExifTool timeout on {target_path}")
            return [], [], [], []
        except json.JSONDecodeError as e:
            logging.error(f"read_all_metadata\tJSON decode error for\t{target_path}:\t{e}")
            return [], [], [], []
        except Exception as e:
            logging.error(f"read_all_metadata\tUnexpected error reading\t{target_path}:\t{str(e)}")
            return [], [], [], []

    # Function to normalize values to a list for consistent processing
    def normalize_to_list(value):
        if value is None:
            return []
        elif isinstance(value, list):
            return value
        elif isinstance(value, str):
            return [value]
        else:
            return []

    # Try original file first
    faces, keywords, subject, hierarchical = read_all_metadata(file_path)
    if faces or keywords or subject or hierarchical:
        return faces, keywords, subject, hierarchical

    # Check for XMP sidecar if original file has no metadata
    file_ext = os.path.splitext(file_path)[1].lower().lstrip('.')
    if file_ext in RAW_FORMATS:
        sidecar = os.path.splitext(file_path)[0] + ".xmp"
        if os.path.isfile(sidecar):
            if logging.getLogger().isEnabledFor(logging.INFO):
                logging.info(f"Reading XMP sidecar for\t{file_path}")
            return read_all_metadata(sidecar)

    return [], [], [], []

# Function to extract face regions from metadata with optimized parsing
def extract_faces_from_data(data) -> List[Tuple[str, float, float, float, float]]:

    existing_faces = []

    # Safely convert value to float with error handling
    def safe_float_convert(value):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    # Try flat keys first (faster path for most formats)
    names = data.get('RegionName')
    types = data.get('RegionType')
    coords = [data.get(key) for key in ['RegionAreaX', 'RegionAreaY', 'RegionAreaW', 'RegionAreaH']]
    
    if names and types and all(coord is not None for coord in coords):
        xs, ys, ws, hs = coords
        
        # Normalize all to lists
        for arr in [names, types, xs, ys, ws, hs]:
            if not isinstance(arr, list):
                arr = [arr]
        
        # Ensure all arrays have same length
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

    # Try nested region structure (XMP format)
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
                
                # Try direct coordinate fields first
                coords = [reg.get(key) for key in ['RegionAreaX', 'RegionAreaY', 'RegionAreaW', 'RegionAreaH']]
                
                # If not found, try nested Area structure
                if any(coord is None for coord in coords):
                    area = reg.get('Area')
                    if isinstance(area, dict):
                        coords = [area.get(key) for key in ['X', 'Y', 'W', 'H']]
                
                # Convert coordinates safely
                x, y, w, h = [safe_float_convert(coord) for coord in coords]
                
                if all(v is not None for v in [x, y, w, h]):
                    existing_faces.append((name, x, y, w, h))
    
    return existing_faces

# Function to write metadata in batch with optimized argument handling and error management.
def write_metadata_batch(image_path, face_regions: List[Tuple], keywords_to_add: Dict[str, List[str]], 
                        dry_run: bool, exiftool_path: str, use_sidecar: bool):

    if not face_regions and not any(keywords_to_add.values()):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f"write_metadata_batch\tskipping write for\t{image_path}\t no face regions or keywords to add")
        return
    
    args = [exiftool_path, '-overwrite_original', '-fast']  # Added fast mode
    target = image_path
    
    # Set up sidecar writing if needed
    if use_sidecar:
        sidecar_path = os.path.splitext(image_path)[0] + ".xmp"
        args.extend(['-use', 'MWG', '-tagsFromFile', '@', '-all:all'])
        target = sidecar_path
    
    # Build face region arguments efficiently
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
    
    # Build keyword arguments efficiently
    keyword_mappings = {
        'keywords': '-XMP-dc:Keywords' if use_sidecar else '-Keywords',
        'subject': '-XMP-dc:Subject' if use_sidecar else '-Subject',
        'hierarchical': '-XMP-lr:HierarchicalSubject' if use_sidecar else '-HierarchicalSubject'
    }
    
    for field, prefix in keyword_mappings.items():
        for keyword in keywords_to_add.get(field, []):
            args.append(f'{prefix}+={keyword}')
    
    args.append(target)
    
    if logging.getLogger().isEnabledFor(logging.DEBUG):
        logging.debug(f"write_metadata_batch\tBatch write:\t{' '.join(args)}")
    
    if not dry_run:
        try:
            result = subprocess.run(
                args,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=30,
                check=False
            )
            
            if result.returncode != 0:
                logging.error(f"write_metadata_batch\tExifTool error writing to {target}:\t{result.stderr.strip()}")
                return False
            else:
                if logging.getLogger().isEnabledFor(logging.INFO):
                    logging.info(f"Wrote metadata to\t{target}")
                return True
                
        except subprocess.TimeoutExpired:
            logging.error(f"write_metadata_batch\tTimeout writing to\t{image_path}")
            return False
        except Exception as e:
            logging.error(f"write_metadata_batch\tError writing to\t{image_path}:\t{str(e)}")
            return False
    
    return True  # Dry run success

# Function to process a single file with face regions and keywords, optimizing memory usage and performance.
def process_file_keywords(full_path, face_list, args, keyword_hierarchy):

    if not os.path.isfile(full_path):
        logging.error(f"process_file_keywords\tFile not found:\t{full_path}")
        return
    
    # Get file format and determine processing strategy
    fmt = face_list[0][0]
    use_sidecar = fmt.lower() in RAW_FORMATS
    log_type = "sidecar" if use_sidecar else "embedded"
    
    # Single optimized metadata read
    existing_faces, existing_keywords, existing_subject, existing_hierarchical = extract_existing_metadata(
        full_path, args.exiftool_path
    )
    
    # Pre-allocate collections for better performance
    new_face_regions = []
    keywords_to_add = {
        'keywords': [],
        'subject': [], 
        'hierarchical': []
    }
    
    # Convert existing collections to sets for O(1) lookup
    existing_keywords_set = set(existing_keywords)
    existing_subject_set = set(existing_subject)
    existing_hierarchical_set = set(existing_hierarchical)
    
    # Process faces efficiently
    for fmt, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy in face_list:
        if logging.getLogger().isEnabledFor(logging.WARNING):   
            logging.warning(f"Processing\t{name}")
        
        # Check for duplicate faces
        dup_type = is_duplicate(existing_faces, name, cw, ch, cx, cy)
        if dup_type:
            if logging.getLogger().isEnabledFor(logging.INFO):
                logging.info(f"Duplicate\t({dup_type}):\t{name}")
            continue
        
        # Add face region to batch
        new_face_regions.append((name, cx, cy, cw, ch))
        logging.info(f"Queued Face\t'{name}'")
        
        # Handle hierarchical keywords efficiently
        if args.write_hierarchical_tags and keyword_id in keyword_hierarchy:
            hierarchical_keyword = keyword_hierarchy[keyword_id]
            simple_keyword = hierarchical_keyword.split('|')[-1] if '|' in hierarchical_keyword else hierarchical_keyword
            
            # Process each keyword field independently with set-based deduplication
            if simple_keyword not in existing_keywords_set:
                keywords_to_add['keywords'].append(simple_keyword)
                existing_keywords_set.add(simple_keyword)  # Prevent duplicates within batch
                logging.info(f"Queued Keywords field\t'{simple_keyword}'")
            
            if simple_keyword not in existing_subject_set:
                keywords_to_add['subject'].append(simple_keyword)
                existing_subject_set.add(simple_keyword)
                logging.info(f"Queued Subject field\t'{simple_keyword}'")
                
            if hierarchical_keyword not in existing_hierarchical_set:
                keywords_to_add['hierarchical'].append(hierarchical_keyword)
                existing_hierarchical_set.add(hierarchical_keyword)
                logging.info(f"Queued HierarchicalSubject field\t'{hierarchical_keyword}'")
    
    # Perform batch write if there's anything to write
    total_keywords = sum(len(v) for v in keywords_to_add.values())
    if new_face_regions or total_keywords > 0:
        success = write_metadata_batch(
            full_path, 
            new_face_regions, 
            keywords_to_add,
            dry_run=not args.write,
            exiftool_path=args.exiftool_path,
            use_sidecar=use_sidecar
        )
        
        if success:
            action = "Wrote" if args.write else "Simulated write"
            face_count = len(new_face_regions)
            if logging.getLogger().isEnabledFor(logging.WARNING):
                logging.warning(f"{action} ({log_type}) batch:\t{face_count} faces,\t{total_keywords} total keywords to\t{full_path}")
            
            # Log breakdown by field for debugging
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                for field, values in keywords_to_add.items():
                    if values:
                        logging.debug(f"\t{field}:\t{len(values)} keywords")

def main():
    global db_pool
    
    args = parse_args()
    session_id = uuid.uuid4().hex[:8]
    init_logging(args.log, session_id, args.log_level)
    if logging.getLogger().isEnabledFor(logging.WARNING):
        logging.warning(f'Session\t{session_id}\tstarted')
    
    # Initialize database pool
    db_pool = DatabasePool(args.catalog)
    
    def run_sync():
        try:
            # Load keyword hierarchy if hierarchical tags are requested
            if logging.getLogger().isEnabledFor(logging.WARNING):
                logging.warning(f'Loading keyword hierarchy from\t{args.catalog}')
            keyword_hierarchy = {}
            if args.write_hierarchical_tags:
                keyword_hierarchy = fetch_keyword_hierarchy(args.catalog, args.batch_size)
                if logging.getLogger().isEnabledFor(logging.WARNING):
                    logging.warning(f'Loaded\t{len(keyword_hierarchy)} keyword hierarchies')
            
            # Fetch face data from the catalog with batch processing
            if logging.getLogger().isEnabledFor(logging.WARNING):
                logging.warning(f'Fetching face data from\t{args.catalog}')
            face_data = fetch_face_data_batch(args.catalog, args.batch_size)
            if logging.getLogger().isEnabledFor(logging.WARNING):
                logging.warning(f'face regions:\t{len(face_data)}\tfound in\t{args.catalog}')
            
            if not face_data:
                logging.error("No face data found in catalog")
                return
            
        finally:
            # Clean up database connections
            if db_pool:
                db_pool.close_all()

        # Group face data by file path for batch processing
        file_data = defaultdict(list)
        for row in face_data:
            full_path, fmt, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy = row
            file_data[full_path].append((fmt, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy))
        
        if logging.getLogger().isEnabledFor(logging.WARNING):
            logging.warning(f'Processing\t{len(file_data)} unique image files')
        
        # Process each file with progress tracking
        for full_path, face_list in tqdm(file_data.items(), desc="Processing images", unit="files"):
            if logging.getLogger().isEnabledFor(logging.WARNING):
                logging.warning(f"======")
                logging.warning(f"File:\t{full_path}")
            process_file_keywords(full_path, face_list, args, keyword_hierarchy)
                
    # Run the main processing function with optional profiling
    if args.profile:
        if logging.getLogger().isEnabledFor(logging.WARNING):
            logging.warning("Profiling enabled")
        profiler = cProfile.Profile()
        profiler.enable()
        
        try:
            run_sync()
        finally:
            profiler.disable()
            
            # Save profiling results
            profile_file = args.profile if isinstance(args.profile, str) else 'profile.prof'
            profiler.dump_stats(profile_file)
            print(f"Profiling data saved to {profile_file}")
            
            # Print top profiling results
            s = io.StringIO()
            ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
            ps.print_stats(20)  # Show top 20 functions
            print("Top profiling results:\n", s.getvalue())
    else:
        run_sync()

    logging.warning(f"Session {session_id} complete")

if __name__ == '__main__':
    main()