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
from typing import List, Tuple, Dict
from collections import defaultdict

# RAW formats that typically require XMP sidecars
RAW_FORMATS = {'cr2', 'cr3', 'nef', 'arw', 'rw2', 'orf', 'raf', 'dng', 'pef', 'sr2'}

# Parse command line arguments
def parse_args():
    parser = argparse.ArgumentParser(description="Sync Lightroom face data to image metadata.")
    parser.add_argument('--catalog', required=True, help='Path to Lightroom .lrcat file')
    parser.add_argument('--log', default='log.txt', help='Path to save the log file')
    parser.add_argument('--write', action='store_true', help='Enable actual writing (default is dry-run)')
    parser.add_argument('--exiftool-path', default='exiftool', help='Path to the exiftool executable')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], help='Set the logging level')
    parser.add_argument('--write-hierarchical-tags', action='store_true', help='Write hierarchical keyword tags to metadata')
    parser.add_argument('--profile', nargs='?', const='profile.prof', default=None, help='Enable profiling and save output to specified file (default: profile.prof)')
    return parser.parse_args()

# Initialize logging
def init_logging(log_path: str, session_id: str, log_level: str):
    logging.basicConfig(
        filename=log_path,
        level=getattr(logging, log_level.upper(), logging.INFO),
        format=f'%(asctime)s\t[Session {session_id}]\t%(levelname)s:\t%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

# Extract keyword hierarchy using recursive CTE
def fetch_keyword_hierarchy(catalog_path: str) -> Dict[int, str]:
    try:
        conn = sqlite3.connect(catalog_path)
        cursor = conn.cursor()
        cursor.execute("""
            WITH RECURSIVE path_builder(id_local, name, parent, full_path) AS (
            -- Base case: root nodes (parent IS NULL), assign empty string if name IS NULL
            SELECT 
                id_local,
                name,
                parent,
                CASE WHEN name IS NULL THEN '' ELSE name END AS full_path
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
                END AS full_path
            FROM AgLibraryKeyword LK
            JOIN path_builder pb ON LK.parent = pb.id_local
            WHERE LK.name IS NOT NULL
            )
            SELECT
            id_local,
            name,
            parent,
            full_path
            FROM path_builder
            WHERE full_path != ''
            ORDER BY full_path;
        """)
        
        rows = cursor.fetchall()
        conn.close()
        logging.info(f"fetch_keyword_hierarchy\tSQLite rows fetched: {len(rows)}")  

        # Fetch results and build hierarchy dictionary
        hierarchy = {}
        for row in rows:
            id_local, name, parent, full_path = row
            hierarchy[id_local] = full_path
            logging.debug(f"Keyword hierarchy: {id_local} -> {full_path}")
        
        return hierarchy

    except sqlite3.OperationalError as e:
        logging.error(f"fetch_keyword_hierarchy\tOperational error accessing database: {e}")
    except sqlite3.DatabaseError as e:
        logging.error(f"fetch_keyword_hierarchy\tDatabase error: {e}")
    except Exception as e:
        logging.error(f"fetch_keyword_hierarchy\tUnexpected error: {e}")
    finally:
        if conn:
            conn.close()

    # Return empty dict if an error occurred
    return {}

# Fetch face data from the Lightroom catalog
def fetch_face_data(catalog_path: str) -> List[Tuple]:
    try:
        conn = sqlite3.connect(catalog_path)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                LRF.absolutePath rootFile,
                LFi.baseName || '.' || LFi.extension fileName, 
                LFo.pathFromRoot,
                LFi.extension,
                LK.name,
                LK.id_local,
                LF.tl_x 'left',
                LF.tl_y 'top', 
                LF.br_x 'right', 
                LF.br_y 'bottom',
                (LF.br_x - LF.tl_x) AS cw,
                (LF.br_y - LF.tl_y) AS ch,
                (LF.tl_x + (LF.br_x - LF.tl_x) / 2.0) AS cx,
                (LF.tl_y + (LF.br_y - LF.tl_y) / 2.0) AS cy
            FROM
                AgLibraryKeyword LK
                INNER JOIN AgLibraryKeywordFace LKF ON LK.id_local = LKF.tag
                INNER JOIN AgLibraryFace LF ON LKF.face = LF.id_local 
                INNER JOIN Adobe_images AI ON AI.id_local = LF.image
                INNER JOIN AgLibraryFile LFi ON AI.rootFile = LFi.id_local
                INNER JOIN AgLibraryFolder LFo ON LFi.folder = LFo.id_local 
                INNER JOIN AgLibraryRootFolder LRF ON LFo.rootFolder = LRF.id_local
            ORDER BY 
                LRF.absolutePath ASC, 
                LFi.baseName ASC, 
                LFi.extension ASC, 
                LK.name ASC;
        """)
        rows = cursor.fetchall()
        conn.close()
        logging.info(f"fetch_face_data\tSQLite rows fetched: {len(rows)}")  

        # Normalize and prepare results
        results = []
        for rootPath, fileName, folderPath, ext, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy in rows:
            full_path = os.path.normpath(os.path.join(rootPath, folderPath, fileName))
            results.append((full_path, ext, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy))
            logging.debug(f"DB Face:\t{full_path}\t{ext}\t{name}\t{keyword_id}\t{left}\t{top}\t{right}\t{bottom}\t{cw}\t{ch}\t{cx}\t{cy}")
        return results

    except sqlite3.OperationalError as e:
        logging.error(f"fetch_face_data\tOperational error accessing database: {e}")
    except sqlite3.DatabaseError as e:
        logging.error(f"fetch_face_data\tDatabase error: {e}")
    except Exception as e:
        logging.error(f"fetch_face_data\tUnexpected error: {e}")
    finally:
        if conn:
            conn.close()

    # Return empty dict if an error occurred
    return {}


# Check if a face region is a duplicate based on name or area
def is_duplicate(existing, name, cw, ch, cx, cy):
    for e_name, ex, ey, ew, eh in existing:
        if name == e_name:
            return 'name'
        if abs(cx - ex) < 0.02 and abs(cy - ey) < 0.02:
            return 'area'
    return None

# Optimized function to extract both face regions and keywords in a single ExifTool call
def extract_existing_metadata(file_path, exiftool_path) -> Tuple[List[Tuple[str, float, float, float, float]], List[str], List[str], List[str]]:
    """
    Extract existing face regions and keywords from image metadata in a single ExifTool call.
    
    Returns:
        Tuple of (faces, keywords, subject, hierarchical_subject)
        - faces: List of tuples (name, x, y, w, h) for face regions
        - keywords: List of keyword strings
        - subject: List of subject strings  
        - hierarchical_subject: List of hierarchical subject strings
    """
    def read_all_metadata(target_path):
        # Single ExifTool call to get all needed metadata
        cmd = [exiftool_path, '-j', '-struct', 
               '-Keywords', '-Subject', '-HierarchicalSubject',
               # Face region fields
               '-RegionName', '-RegionType', '-RegionAreaX', '-RegionAreaY', 
               '-RegionAreaW', '-RegionAreaH', '-RegionInfo',
               target_path]
        
        logging.debug(f"Read all metadata:\t{' '.join(cmd)}")
        
        try:
            result = subprocess.run(cmd,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    text=True,
                                    timeout=15)  # Increased timeout for single comprehensive call
            
            if result.returncode != 0:
                logging.error(f"extract_existing_metadata\tExifTool error on {target_path}: {result.stderr.strip()}")
                return [], [], [], []

            data_list = json.loads(result.stdout)
            if not data_list:
                logging.error(f"extract_existing_metadata\tExifTool returned no JSON for {target_path}")
                return [], [], [], []

            data = data_list[0]
            
            # Extract face regions
            existing_faces = extract_faces_from_data(data)
            
            # Extract keywords
            keywords = data.get('Keywords', [])
            subject = data.get('Subject', [])
            hierarchical = data.get('HierarchicalSubject', [])
            
            # Normalize keyword fields to lists
            if isinstance(keywords, str):
                keywords = [keywords]
            if isinstance(subject, str):
                subject = [subject]
            if isinstance(hierarchical, str):
                hierarchical = [hierarchical]
                
            return existing_faces, keywords or [], subject or [], hierarchical or []
            
        except subprocess.TimeoutExpired:
            logging.error(f"extract_existing_metadata\tExifTool timeout on {target_path}")
            return [], [], [], []
        except json.JSONDecodeError:
            logging.error(f"extract_existing_metadata\tFailed to parse JSON from ExifTool output for {target_path}")
            return [], [], [], []
        except Exception as e:
            logging.error(f"extract_existing_metadata\tUnexpected error reading {target_path}: {str(e)}")
            return [], [], [], []

    # First try original file
    faces, keywords, subject, hierarchical = read_all_metadata(file_path)
    if faces or keywords or subject or hierarchical:
        return faces, keywords, subject, hierarchical

    # If no metadata and sidecar exists, try .xmp
    sidecar = os.path.splitext(file_path)[0] + ".xmp"
    if os.path.isfile(sidecar):
        logging.info(f"Read XMP sidecar for {file_path}")
        return read_all_metadata(sidecar)

    return [], [], [], []


def extract_faces_from_data(data) -> List[Tuple[str, float, float, float, float]]:
    """
    Extract face regions from ExifTool JSON data.
    
    Args:
        data: JSON data from ExifTool
        
    Returns:
        List of tuples (name, x, y, w, h) for face regions
    """
    existing_faces = []

    # Try flat keys first (some formats store region data as flat arrays)
    names = data.get('RegionName')
    types = data.get('RegionType')
    xs = data.get('RegionAreaX')
    ys = data.get('RegionAreaY')
    ws = data.get('RegionAreaW')
    hs = data.get('RegionAreaH')
    
    if names and types and xs and ys and ws and hs:
        # Ensure all are lists for consistent processing
        if not isinstance(names, list):
            names = [names]
        if not isinstance(types, list):
            types = [types]
        if not isinstance(xs, list):
            xs = [xs]
        if not isinstance(ys, list):
            ys = [ys]
        if not isinstance(ws, list):
            ws = [ws]
        if not isinstance(hs, list):
            hs = [hs]
            
        for name, type_, x, y, w, h in zip(names, types, xs, ys, ws, hs):
            if type_ != 'Face':
                continue
            try:
                existing_faces.append((name, float(x), float(y), float(w), float(h)))
            except (ValueError, TypeError):
                logging.warning(f"extract_faces_from_data\tInvalid face region data: {name}, {x}, {y}, {w}, {h}")
                continue
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
                
                # Get coordinates - try direct fields first
                x = reg.get('RegionAreaX')
                y = reg.get('RegionAreaY')
                w = reg.get('RegionAreaW')
                h = reg.get('RegionAreaH')
                
                # If not found, try nested Area structure
                if None in (x, y, w, h):
                    area = reg.get('Area')
                    if isinstance(area, dict):
                        x = area.get('X')
                        y = area.get('Y')
                        w = area.get('W')
                        h = area.get('H')
                
                # Skip if we don't have all required data
                if None in (name, type_, x, y, w, h):
                    continue
                if type_ != 'Face':
                    continue
                    
                try:
                    existing_faces.append((name, float(x), float(y), float(w), float(h)))
                except (ValueError, TypeError):
                    logging.warning(f"extract_faces_from_data\tInvalid nested face region data: {name}, {x}, {y}, {w}, {h}")
                    continue
    
    return existing_faces

# Batch write all metadata to image using single ExifTool call
def write_metadata_batch(image_path, face_regions: List[Tuple], keywords_to_add: Dict[str, List[str]], dry_run: bool, exiftool_path: str, use_sidecar: bool):
    """
    Write all face regions and keywords to image in a single ExifTool call.
    
    Args:
        image_path: Path to the image file
        face_regions: List of tuples (name, x, y, w, h) for face regions to write
        keywords_to_add: Dict with keys 'keywords', 'subject', 'hierarchical' containing lists of values to add
        dry_run: If True, don't actually write metadata
        exiftool_path: Path to ExifTool executable
        use_sidecar: If True, write to XMP sidecar file
    """
    if not face_regions and not any(keywords_to_add.values()):
        return
    
    args = [exiftool_path, '-overwrite_original']
    target = image_path
    
    # If using sidecar, set up XMP sidecar arguments
    if use_sidecar:
        sidecar_path = os.path.splitext(image_path)[0] + ".xmp"
        args += [
            '-use', 'MWG',
            '-tagsFromFile', '@',
            '-all:all'
        ]
        target = sidecar_path
    
    # Add face region arguments
    for name, x, y, w, h in face_regions:
        if use_sidecar:
            args += [
                f'-XMP-mwg-rs:RegionName+={name}',
                f'-XMP-mwg-rs:RegionType+=Face',
                f'-XMP-mwg-rs:RegionAreaX+={x}',
                f'-XMP-mwg-rs:RegionAreaY+={y}',
                f'-XMP-mwg-rs:RegionAreaW+={w}',
                f'-XMP-mwg-rs:RegionAreaH+={h}'
            ]
        else:
            args += [
                f'-RegionName+={name}',
                f'-RegionType+=Face',
                f'-RegionAreaX+={x}',
                f'-RegionAreaY+={y}',
                f'-RegionAreaW+={w}',
                f'-RegionAreaH+={h}'
            ]
    
    # Add keyword arguments for each field independently
    for simple_keyword in keywords_to_add.get('keywords', []):
        field_prefix = '-XMP-dc:' if use_sidecar else '-'
        args.append(f'{field_prefix}Keywords+={simple_keyword}')
    
    for subject_keyword in keywords_to_add.get('subject', []):
        field_prefix = '-XMP-dc:' if use_sidecar else '-'
        args.append(f'{field_prefix}Subject+={subject_keyword}')
        
    for hierarchical_keyword in keywords_to_add.get('hierarchical', []):
        field_prefix = '-XMP-lr:' if use_sidecar else '-'
        args.append(f'{field_prefix}HierarchicalSubject+={hierarchical_keyword}')
    
    args.append(target)
    logging.info(f"Batch write:\t{' '.join(args)}")
    
    if not dry_run:
        try:
            result = subprocess.run(args,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    text=True,
                                    timeout=30)
            if result.returncode != 0:
                logging.error(f"write_metadata_batch\tExifTool error writing to {target}: {result.stderr.strip()}")
            else:
                logging.debug(f"write_metadata_batch\tSuccessfully wrote metadata to {target}")
        except Exception as e:
            logging.error(f"write_metadata_batch\tError writing to {image_path}: {str(e)}")

# Updated process_file_keywords function to use the optimized metadata reading
def process_file_keywords(full_path, face_list, args, keyword_hierarchy):
    """Process a single file's keywords and faces with optimized single metadata read"""
    
    if not os.path.isfile(full_path):
        logging.error(f"File not found:\t{full_path}")
        return
    
    # Get file format from first face entry
    fmt = face_list[0][0]
    use_sidecar = fmt.lower() in RAW_FORMATS
    log_type = "sidecar" if use_sidecar else "embedded"
    
    # Single call to read all existing metadata
    existing_faces, existing_keywords, existing_subject, existing_hierarchical = extract_existing_metadata(
        full_path, args.exiftool_path
    )
    
    # Collect new face regions and keywords to write
    new_face_regions = []
    keywords_to_add = {
        'keywords': [],
        'subject': [], 
        'hierarchical': []
    }
    
    for fmt, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy in face_list:
        logging.warning(f"Processing\t{name}")
        
        # Check for duplicate faces
        dup_type = is_duplicate(existing_faces, name, cw, ch, cx, cy)
        if dup_type:
            logging.warning(f"Duplicate\t({dup_type}):\t{name}")
        else:
            # Add face region to batch
            new_face_regions.append((name, cx, cy, cw, ch))
            logging.warning(f"Queued face\t'{name}'\tfor batch write")
        
        # Handle hierarchical keywords independently
        if args.write_hierarchical_tags and keyword_id in keyword_hierarchy:
            hierarchical_keyword = keyword_hierarchy[keyword_id]
            simple_keyword = hierarchical_keyword.split('|')[-1] if '|' in hierarchical_keyword else hierarchical_keyword
            
            # Check Keywords field independently
            if simple_keyword not in existing_keywords and simple_keyword not in keywords_to_add['keywords']:
                keywords_to_add['keywords'].append(simple_keyword)
                existing_keywords.append(simple_keyword)  # Prevent duplicates within same batch
                logging.warning(f"Queued Keywords field\t'{simple_keyword}'\tfor batch write")
            else:
                logging.warning(f"Existing in Keywords field:\t{simple_keyword}")
            
            # Check Subject field independently  
            if simple_keyword not in existing_subject and simple_keyword not in keywords_to_add['subject']:
                keywords_to_add['subject'].append(simple_keyword)
                existing_subject.append(simple_keyword)  # Prevent duplicates within same batch
                logging.warning(f"Queued Subject field\t'{simple_keyword}'\tfor batch write")
            else:
                logging.warning(f"Existing in Subject field:\t{simple_keyword}")
                
            # Check HierarchicalSubject field independently
            if hierarchical_keyword not in existing_hierarchical and hierarchical_keyword not in keywords_to_add['hierarchical']:
                keywords_to_add['hierarchical'].append(hierarchical_keyword)
                existing_hierarchical.append(hierarchical_keyword)  # Prevent duplicates within same batch
                logging.warning(f"Queued HierarchicalSubject field\t'{hierarchical_keyword}'\tfor batch write")
            else:
                logging.warning(f"Existing in HierarchicalSubject field:\t{hierarchical_keyword}")
    
    # Perform batch write if there's anything to write
    total_keywords = sum(len(v) for v in keywords_to_add.values())
    if new_face_regions or total_keywords > 0:
        write_metadata_batch(
            full_path, 
            new_face_regions, 
            keywords_to_add,
            dry_run=not args.write,
            exiftool_path=args.exiftool_path,
            use_sidecar=use_sidecar
        )
        
        action = "Wrote" if args.write else "Simulated write"
        face_count = len(new_face_regions)
        logging.warning(f"{action} ({log_type}) batch: {face_count} faces, {total_keywords} total keywords to {full_path}")
        
        # Log breakdown by field
        for field, values in keywords_to_add.items():
            if values:
                logging.warning(f"  {field}: {len(values)} keywords")

def main():
    args = parse_args()
    session_id = uuid.uuid4().hex[:8]
    init_logging(args.log, session_id, args.log_level)
    logging.warning(f'Session {session_id} started')
    
    def run_sync():
        # Load keyword hierarchy if hierarchical tags are requested
        logging.warning(f'Loading keyword hierarchy from {args.catalog}')
        keyword_hierarchy = {}
        if args.write_hierarchical_tags:
            keyword_hierarchy = fetch_keyword_hierarchy(args.catalog)
            logging.warning(f'Loaded {len(keyword_hierarchy)} keyword hierarchies')
        
        # Fetch face data from the catalog
        logging.warning(f'Fetching face data from {args.catalog}')
        face_data = fetch_face_data(args.catalog)
        logging.warning(f'{len(face_data)} face regions found in {args.catalog}')
        
        # Group face data by file path for batch processing
        file_data = defaultdict(list)
        for row in face_data:
            full_path, fmt, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy = row
            file_data[full_path].append((fmt, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy))
        
        # Process each file
        for full_path, face_list in tqdm(file_data.items(), desc="Processing images"):
            logging.warning(f"======")
            logging.warning(f"File:\t{full_path}")
            process_file_keywords(full_path, face_list, args, keyword_hierarchy)

    # Run the main processing function
    if args.profile:
        # Enable profiling if requested
        logging.warning("Profiling enabled")
        profiler = cProfile.Profile()
        profiler.enable()
        run_sync()
        profiler.disable()
        # Save profiling results to a file
        profile_file = args.profile if isinstance(args.profile, str) else 'profile.prof'
        profiler.dump_stats(profile_file)
        print(f"Profiling data saved to {profile_file}")        
        # Print profiling results to console
        s = io.StringIO()
        ps = pstats.Stats(profiler, stream=s).sort_stats('cumulative')
        ps.print_stats()
        print("Profiling results:\n", s.getvalue())
    else:
        # Run without profiling
        run_sync()

    logging.warning(f"Session {session_id} complete")

if __name__ == '__main__':
    main()
# This script is intended to be run as a standalone program
# and will not execute if imported as a module.