# Script to sync Lightroom face data to image metadata using ExifTool
# Based on the database schema from: Lightroom Classic 14.3
# Optimized version with improved SQL queries and database access
# Copyright (c) 2025, Massimo Savazzi
# All rights reserved.
# This script is licensed under the MIT License.

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

def init_logging(log_path: str, session_id: str, log_level: str):
    logging.basicConfig(
        filename=log_path,
        level=getattr(logging, log_level.upper(), logging.INFO),
        format=f'%(asctime)s\t[Session {session_id}]\t%(levelname)s:\t%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

# Optimized keyword hierarchy extraction using prepared statements and indexing hints.
def fetch_keyword_hierarchy(catalog_path: str) -> Dict[int, str]:

    global db_pool
    hierarchy = {}
    
    try:
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            
            # First, ensure we have proper indexes for performance
            #cursor.execute("""
            #    CREATE INDEX IF NOT EXISTS idx_keyword_parent ON AgLibraryKeyword(parent, id_local);
            #""")
            #cursor.execute("""
            #    CREATE INDEX IF NOT EXISTS idx_keyword_name ON AgLibraryKeyword(name, id_local);
            #""")
            
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
            
            rows = cursor.fetchall()
            if logging.getLogger().isEnabledFor(logging.INFO):
                logging.info(f"fetch_keyword_hierarchy\tSQLite rows fetched: {len(rows)}")
            
            # Build hierarchy dictionary with better memory efficiency
            for id_local, full_path in rows:
                hierarchy[id_local] = full_path
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.debug(f"Keyword hierarchy: {id_local} -> {full_path}")
            
    except sqlite3.Error as e:
        logging.error(f"fetch_keyword_hierarchy\tDatabase error: {e}")
        return {}
    except Exception as e:
        logging.error(f"fetch_keyword_hierarchy\tUnexpected error: {e}")
        return {}
    
    return hierarchy

#Optimized face data fetching with proper indexing and batch processing.
def fetch_face_data_batch(catalog_path: str, batch_size: int = 1000) -> List[Tuple]:

    global db_pool
    results = []
    
    try:
        with db_pool.get_connection() as conn:
            cursor = conn.cursor()
            
            # Create indexes for better performance if they don't exist
            #indexes = [
            #    "CREATE INDEX IF NOT EXISTS idx_keywordface_tag ON AgLibraryKeywordFace(tag, face);",
            #    "CREATE INDEX IF NOT EXISTS idx_keywordface_face ON AgLibraryKeywordFace(face, tag);",
            #    "CREATE INDEX IF NOT EXISTS idx_face_image ON AgLibraryFace(image, id_local);",
            #    "CREATE INDEX IF NOT EXISTS idx_images_rootfile ON Adobe_images(rootFile, id_local);",
            #    "CREATE INDEX IF NOT EXISTS idx_file_folder ON AgLibraryFile(folder, id_local);",
            #    "CREATE INDEX IF NOT EXISTS idx_folder_root ON AgLibraryFolder(rootFolder, id_local);"
            #]
            
            #for idx_sql in indexes:
            #    cursor.execute(idx_sql)
            
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
            while True:
                batch = cursor.fetchmany(batch_size)
                if not batch:
                    break
                
                for row in batch:
                    (rootPath, fileName, folderPath, ext, name, keyword_id, 
                     left, top, right, bottom, cw, ch, cx, cy) = row
                    
                    # Normalize path once per row
                    full_path = os.path.normpath(os.path.join(rootPath, folderPath, fileName))
                    
                    results.append((full_path, ext, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy))
                    
                    if logging.getLogger().isEnabledFor(logging.DEBUG):
                        logging.debug(f"DB Face:\t{full_path}\t{ext}\t{name}\t{keyword_id}\t{left}\t{top}\t{right}\t{bottom}\t{cw}\t{ch}\t{cx}\t{cy}")
            
            if logging.getLogger().isEnabledFor(logging.INFO):
                logging.info(f"fetch_face_data_batch\tTotal SQLite rows fetched: {len(results)}")
            
    except sqlite3.Error as e:
        logging.error(f"fetch_face_data_batch\tDatabase error: {e}")
        return []
    except Exception as e:
        logging.error(f"fetch_face_data_batch\tUnexpected error: {e}")
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

def extract_existing_metadata(file_path, exiftool_path) -> Tuple[List[Tuple[str, float, float, float, float]], List[str], List[str], List[str]]:
    """
    Optimized metadata extraction with better error handling and caching.
    """
    def read_all_metadata(target_path):
        # Optimized ExifTool command with specific field selection
        cmd = [
            exiftool_path, '-j', '-struct', '-fast2',  # Fast mode for better performance
            '-Keywords', '-Subject', '-HierarchicalSubject',
            '-RegionName', '-RegionType', '-RegionAreaX', '-RegionAreaY', 
            '-RegionAreaW', '-RegionAreaH', '-RegionInfo',
            target_path
        ]
        
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.debug(f"Read all metadata:\t{' '.join(cmd)}")
        
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
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.debug(f"extract_existing_metadata\tExifTool warning on {target_path}: {result.stderr.strip()}")
                return [], [], [], []

            if not result.stdout.strip():
                return [], [], [], []
                
            data_list = json.loads(result.stdout)
            if not data_list:
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
            logging.warning(f"extract_existing_metadata\tExifTool timeout on {target_path}")
            return [], [], [], []
        except json.JSONDecodeError as e:
            logging.error(f"extract_existing_metadata\tJSON decode error for {target_path}: {e}")
            return [], [], [], []
        except Exception as e:
            logging.error(f"extract_existing_metadata\tUnexpected error reading {target_path}: {str(e)}")
            return [], [], [], []

    def normalize_to_list(value):
        """Convert value to list format consistently"""
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
            logging.info(f"Reading XMP sidecar for {file_path}")
            return read_all_metadata(sidecar)

    return [], [], [], []

def extract_faces_from_data(data) -> List[Tuple[str, float, float, float, float]]:
    """
    Optimized face region extraction with better error handling.
    """
    existing_faces = []

    def safe_float_convert(value):
        """Safely convert value to float with error handling"""
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

def write_metadata_batch(image_path, face_regions: List[Tuple], keywords_to_add: Dict[str, List[str]], 
                        dry_run: bool, exiftool_path: str, use_sidecar: bool):
    """
    Optimized batch metadata writing with better error handling and performance.
    """
    if not face_regions and not any(keywords_to_add.values()):
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
        logging.debug(f"Batch write:\t{' '.join(args)}")
    
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
                logging.error(f"write_metadata_batch\tExifTool error writing to {target}: {result.stderr.strip()}")
                return False
            else:
                if logging.getLogger().isEnabledFor(logging.DEBUG):
                    logging.debug(f"write_metadata_batch\tSuccessfully wrote metadata to {target}")
                return True
                
        except subprocess.TimeoutExpired:
            logging.error(f"write_metadata_batch\tTimeout writing to {image_path}")
            return False
        except Exception as e:
            logging.error(f"write_metadata_batch\tError writing to {image_path}: {str(e)}")
            return False
    
    return True  # Dry run success

def process_file_keywords(full_path, face_list, args, keyword_hierarchy):
    """
    Optimized file processing with better memory management and error handling.
    """
    if not os.path.isfile(full_path):
        logging.error(f"File not found:\t{full_path}")
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
        logging.info(f"Processing\t{name}")
        
        # Check for duplicate faces
        dup_type = is_duplicate(existing_faces, name, cw, ch, cx, cy)
        if dup_type:
            logging.info(f"Duplicate\t({dup_type}):\t{name}")
            continue
        
        # Add face region to batch
        new_face_regions.append((name, cx, cy, cw, ch))
        logging.info(f"Queued face\t'{name}'\tfor batch write")
        
        # Handle hierarchical keywords efficiently
        if args.write_hierarchical_tags and keyword_id in keyword_hierarchy:
            hierarchical_keyword = keyword_hierarchy[keyword_id]
            simple_keyword = hierarchical_keyword.split('|')[-1] if '|' in hierarchical_keyword else hierarchical_keyword
            
            # Process each keyword field independently with set-based deduplication
            if simple_keyword not in existing_keywords_set:
                keywords_to_add['keywords'].append(simple_keyword)
                existing_keywords_set.add(simple_keyword)  # Prevent duplicates within batch
                logging.info(f"Queued Keywords field\t'{simple_keyword}'\tfor batch write")
            
            if simple_keyword not in existing_subject_set:
                keywords_to_add['subject'].append(simple_keyword)
                existing_subject_set.add(simple_keyword)
                logging.info(f"Queued Subject field\t'{simple_keyword}'\tfor batch write")
                
            if hierarchical_keyword not in existing_hierarchical_set:
                keywords_to_add['hierarchical'].append(hierarchical_keyword)
                existing_hierarchical_set.add(hierarchical_keyword)
                logging.info(f"Queued HierarchicalSubject field\t'{hierarchical_keyword}'\tfor batch write")
    
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
            logging.info(f"{action} ({log_type}) batch: {face_count} faces, {total_keywords} total keywords to {full_path}")
            
            # Log breakdown by field for debugging
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                for field, values in keywords_to_add.items():
                    if values:
                        logging.debug(f"  {field}: {len(values)} keywords")

def main():
    global db_pool
    
    args = parse_args()
    session_id = uuid.uuid4().hex[:8]
    init_logging(args.log, session_id, args.log_level)
    logging.info(f'Session {session_id} started')
    
    # Initialize database pool
    db_pool = DatabasePool(args.catalog)
    
    def run_sync():
        try:
            # Load keyword hierarchy if hierarchical tags are requested
            logging.info(f'Loading keyword hierarchy from {args.catalog}')
            keyword_hierarchy = {}
            if args.write_hierarchical_tags:
                keyword_hierarchy = fetch_keyword_hierarchy(args.catalog)
                logging.info(f'Loaded {len(keyword_hierarchy)} keyword hierarchies')
            
            # Fetch face data from the catalog with batch processing
            logging.info(f'Fetching face data from {args.catalog}')
            face_data = fetch_face_data_batch(args.catalog, args.batch_size)
            logging.info(f'{len(face_data)} face regions found in {args.catalog}')
            
            if not face_data:
                logging.warning("No face data found in catalog")
                return
            
            # Group face data by file path for batch processing
            file_data = defaultdict(list)
            for row in face_data:
                full_path, fmt, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy = row
                file_data[full_path].append((fmt, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy))
            
            logging.info(f'Processing {len(file_data)} unique image files')
            
            # Process each file with progress tracking
            for full_path, face_list in tqdm(file_data.items(), desc="Processing images", unit="files"):
                logging.info(f"======")
                logging.info(f"File:\t{full_path}")
                process_file_keywords(full_path, face_list, args, keyword_hierarchy)
                
        finally:
            # Clean up database connections
            if db_pool:
                db_pool.close_all()

    # Run the main processing function with optional profiling
    if args.profile:
        logging.info("Profiling enabled")
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

    logging.info(f"Session {session_id} complete")

if __name__ == '__main__':
    main()