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
from tqdm import tqdm
from datetime import datetime
from typing import List, Tuple, Dict

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
                ELSE pb.full_path || '/' || LK.name
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
            results.append((full_path, ext, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy, id_local))
            logging.debug(f"DB Face:\t{full_path}\t{ext}\t{name}\t{keyword_id}\t{left}\t{top}\t{right}\t{bottom}\t{cw}\t{ch}\t{cx}\t{cy}\t{id_local}")
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

# Extract existing face regions from image metadata using ExifTool
def extract_existing_faces(file_path, exiftool_path, prefer_mwg=True) -> List[Tuple[str, float, float, float, float]]:
    def read_faces(target_path):
        cmd = [exiftool_path, '-j', '-struct', target_path]
        logging.debug(f"Read regions:\t{' '.join(cmd)}")
        result = subprocess.run(cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                text=True,
                                timeout=10)
        if result.returncode != 0:
            logging.error(f"extract_existing_faces\tExifTool error on {target_path}: {result.stderr.strip()}")
            return []

        data_list = json.loads(result.stdout)
        if not data_list:
            logging.error(f"extract_existing_faces\tExifTool returned no JSON for {target_path}")
            return []

        data = data_list[0]
        existing = []

        # Flat keys
        names = data.get('RegionName')
        types = data.get('RegionType')
        xs = data.get('RegionAreaX')
        ys = data.get('RegionAreaY')
        ws = data.get('RegionAreaW')
        hs = data.get('RegionAreaH')
        if names and types and xs and ys and ws and hs:
            for name, type_, x, y, w, h in zip(names, types, xs, ys, ws, hs):
                if type_ != 'Face':
                    continue
                existing.append((name, float(x), float(y), float(w), float(h)))
            return existing

        # Nested region list
        region_info = data.get('RegionInfo')
        if isinstance(region_info, dict):
            region_list = region_info.get('RegionList')
            if isinstance(region_list, list):
                for reg in region_list:
                    name = reg.get('RegionName') or reg.get('Name')
                    type_ = reg.get('RegionType') or reg.get('Type')
                    x = reg.get('RegionAreaX')
                    y = reg.get('RegionAreaY')
                    w = reg.get('RegionAreaW')
                    h = reg.get('RegionAreaH')
                    if None in (x, y, w, h):
                        area = reg.get('Area')
                        if isinstance(area, dict):
                            x = area.get('X')
                            y = area.get('Y')
                            w = area.get('W')
                            h = area.get('H')
                    if None in (name, type_, x, y, w, h):
                        continue
                    if type_ != 'Face':
                        continue
                    existing.append((name, float(x), float(y), float(w), float(h)))
        return existing

    # First try original file
    faces = read_faces(file_path)
    if faces:
        return faces

    # If no metadata and sidecar exists, try .xmp
    sidecar = os.path.splitext(file_path)[0] + ".xmp"
    if os.path.isfile(sidecar):
        logging.info(f"Read XMP sidecar for {file_path}")
        return read_faces(sidecar)

    return []

# Extract existing keywords from image metadata using ExifTool
def extract_existing_keywords(file_path, exiftool_path) -> List[str]:
    # Function to read keywords from metadata using ExifTool
    def read_keywords(target_path):
        cmd = [exiftool_path, '-j', '-Keywords', '-Subject', '-HierarchicalSubject', target_path]
        logging.debug(f"Read keywords:\t{' '.join(cmd)}")
        result = subprocess.run(cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                text=True,
                                timeout=10)
        if result.returncode != 0:
            logging.error(f"extract_existing_keywords\tExifTool error reading keywords from {target_path}: {result.stderr.strip()}")
            return []

        try:
            data_list = json.loads(result.stdout)
            if not data_list:
                return []
            
            data = data_list[0]
            existing_keywords = set()
            
            # Check various keyword fields
            for field in ['Keywords', 'Subject', 'HierarchicalSubject']:
                values = data.get(field)
                if values:
                    if isinstance(values, list):
                        existing_keywords.update(values)
                    elif isinstance(values, str):
                        existing_keywords.add(values)
            
            return list(existing_keywords)
        except json.JSONDecodeError:
            logging.error(f"extract_existing_keywords\tFailed to parse JSON from ExifTool keyword output for {target_path}")
            return []

    # First try original file
    keywords = read_keywords(file_path)
    if keywords:
        return keywords

    # If no metadata and sidecar exists, try .xmp
    sidecar = os.path.splitext(file_path)[0] + ".xmp"
    if os.path.isfile(sidecar):
        logging.info(f"Read XMP sidecar for {file_path}")
        return read_keywords(sidecar)

    return []

# Write face region data to metadata using ExifTool
def write_xmp_region(image_path, name, x, y, w, h, dry_run: bool, exiftool_path: str, use_sidecar: bool):
    args = [exiftool_path, '-overwrite_original']
    target = image_path

    # If the image is a RAW format, we will use a sidecar XMP file
    if use_sidecar:
        sidecar_path = os.path.splitext(image_path)[0] + ".xmp"
        args += [
            '-use', 'MWG',
            '-tagsFromFile', '@',
            '-all:all',
            f'-XMP-mwg-rs:RegionName+={name}',
            f'-XMP-mwg-rs:RegionType+=Face',
            f'-XMP-mwg-rs:RegionAreaX+={x}',
            f'-XMP-mwg-rs:RegionAreaY+={y}',
            f'-XMP-mwg-rs:RegionAreaW+={w}',
            f'-XMP-mwg-rs:RegionAreaH+={h}',
            sidecar_path
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

    args.append(target)
    logging.info(f"Write Region:\t{' '.join(args)}")

    if not dry_run:
        try:
            result=subprocess.run(args,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE,
                           text=True,
                           timeout=10)
            if result.returncode != 0:
                logging.error(f"write_xmp_region\tExifTool error writing regions to {target}: {result.stderr.strip()}")
        except Exception as e:
            logging.error(f"write_xmp_region\tError writing to {image_path}: {str(e)}")

# Write hierarchical keyword to metadata using ExifTool
def write_hierarchical_keyword(image_path, hierarchical_keyword: str, keyword: str,dry_run: bool, exiftool_path: str, use_sidecar: bool):
    
    args = [exiftool_path, '-overwrite_original']
    target = image_path

    # If the image is a RAW format, we will use a sidecar XMP file
    if use_sidecar:
        sidecar_path = os.path.splitext(image_path)[0] + ".xmp"
        args += [
            '-use', 'MWG',
            '-tagsFromFile', '@',
            '-all:all',
            f'-Keywords+={hierarchical_keyword}',
            f'-Subject+={hierarchical_keyword}',
            f'-HierarchicalSubject+={hierarchical_keyword}',
            sidecar_path
        ]
    else:
        args += [
            f'-Keywords+={keyword}',
            f'-Subject+={keyword}',
            f'-HierarchicalSubject+={hierarchical_keyword}'
        ]

    args.append(target)
    logging.debug(f"Write keyword:\t{' '.join(args)}")

    if not dry_run:
        try:
            result = subprocess.run(args,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    text=True,
                                    timeout=10)
            if result.returncode != 0:
                logging.error(f"write_hierarchical_keyword\tError writing keyword to {image_path}: {result.stderr.strip()}")
        except Exception as e:
            logging.error(f"write_hierarchical_keyword\tError writing keyword to {image_path}: {str(e)}")

def main():
    args = parse_args()
    session_id = uuid.uuid4().hex[:8]
    init_logging(args.log, session_id, args.log_level)
    logging.warning(f'Session {session_id} started')
    
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
    old_full_path = ""
    existing_faces = []
    existing_keywords = []

    # Process each face region and metadata
    for row in tqdm(face_data, desc="Processing images"):
        full_path, fmt, name, keyword_id, left, top, right, bottom, cw, ch, cx, cy = row
        if full_path != old_full_path:
            logging.warning(f"======")
            logging.warning(f"File:\t{full_path}\tFormat:\t{fmt}")
        else:
            logging.warning(f"------")
        if not os.path.isfile(full_path):
            logging.error(f"File not found:\t{full_path}")
            continue
        else:
            logging.warning(f"Processing\t{name}")

        # Only re-read existing data when we encounter a new file
        if full_path != old_full_path:
            old_full_path = full_path
            existing_faces = []
            existing_faces = extract_existing_faces(full_path, args.exiftool_path)
            existing_keywords = []
            if args.write_hierarchical_tags:
                existing_keywords = extract_existing_keywords(full_path, args.exiftool_path)

        # Check for duplicate faces
        dup_type = is_duplicate(existing_faces, name, cw, ch, cx, cy)
        if dup_type:
            logging.warning(f"Duplicate\t({dup_type}):\t{name}")
        else:
            # Write face region data
            use_sidecar = fmt.lower() in RAW_FORMATS
            log_type = "sidecar" if use_sidecar else "embedded"

            write_xmp_region(full_path, name, cx, cy, cw, ch,
                             dry_run=not args.write,
                             exiftool_path=args.exiftool_path,
                             use_sidecar=use_sidecar)

            action = "Wrote" if args.write else "Simulated write"
            logging.warning(f"{action} ({log_type}) face\t'{name}'\tto\t{full_path} ")

        # Write hierarchical keyword tag if requested and not already present
        if args.write_hierarchical_tags and keyword_id in keyword_hierarchy:
            hierarchical_keyword = keyword_hierarchy[keyword_id]
            
            # Check if this hierarchical keyword already exists
            if hierarchical_keyword not in existing_keywords:
                use_sidecar = fmt.lower() in RAW_FORMATS
                log_type = "sidecar" if use_sidecar else "embedded"
                
                write_hierarchical_keyword(full_path, hierarchical_keyword, 
                                         name,
                                         dry_run=not args.write,
                                         exiftool_path=args.exiftool_path,
                                         use_sidecar=use_sidecar)
                
                action = "Wrote" if args.write else "Simulated write"
                logging.warning(f"{action} ({log_type}) keyword\t'{name}'\t hierarchical keyword:\t'{hierarchical_keyword}'\tto\t{full_path}")
                
                # Add to existing keywords to avoid re-writing in same session
                existing_keywords.append(hierarchical_keyword)
            else:
                logging.warning(f"Existing:\t{hierarchical_keyword}")

    logging.warning(f"Session {session_id} complete")

if __name__ == '__main__':
    main()