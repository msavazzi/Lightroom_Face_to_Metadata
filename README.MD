# Info

  

**Script write Lightroom face data and tags to image metadata using ExifTool**


*This script is intended for educational purposes only. Use at your own risk.*

*It is recommended to backup your Lightroom catalog and images before running this script.*

*This script is provided "as is" without warranty of any kind, either expressed or implied.*

*The author is not responsible for any damage or loss of data that may occur as a result of using this script.*
   
  

Based on the database schema from: ***Lightroom Classic 14.3*** analyzed as *SQLite 3 Database*

  

The usage of python and ExifTool is needed as Lightroom does not write face data to metadata nor it provides a way to export it via Plugin API.
The script queries the database for all the files with Faces and extracts all the regions as defined in Lightroom, this is done in memory; it also queries all the metadata related to faces and their hierarchy.
For every file with faces it extracts the metadata (Subject, HirearchicalSubject, PersonInImage) from the image or the XMP sidecart .xmp file and compares the Face from database with the ones in metadata, it checks for the presence of a IPCT block to extract Keywords tag.
On the face the check to avoid duplicates is done on the name and on the region verifying there are not regions fully overlapping.
If the Face is not present in the file it is added to the XMP metadata. 
The metadata is checked for duplicates, based on name, and  will add the missing Face keywords accordingly.
The HierarchicalSubject is the full metadata hierarchy, separated by |.
The script manages flat metadata (as in .heic files) and hierarchical regions (as in .jpg files).
Highly optimized with threads for speed and memory, using a single exiftool subprocess per thread.
If executed without any flag a GUI is shown, as well as if requested from command line; at run configuration parameters are saved (Lightroom_Face_to_Metadata_last_run.json) and re loaded on next run as defaults for options not specified in the command line.

  

Options are:

    --catalog		            Path to Lightroom .lrcat file
    --log			            Path to save the log file - default log.txt in the same folder of the script
    --write			            Enable actual writing (default is dry-run)
    --exiftool-path                     Path to the exiftool executable - default is exiftool in the same folder of the script
    --log-level		            Set the logging level - default is 'INFO', choices are 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    --tags                              Write hierarchical keyword tags to metadata
    --profile                           Enable profiling and save output to specified file (default: profile.prof)
    --batch-size                        Database query batch size (default: 1000)'
    --threads                           Number of threads for file operations (0 = auto-detect optimal)
    --max-threads                       Maximum number of threads to use when auto-detecting (default: 16)
    --gui                               Shows the GUI

Usage Examples:


**Basic usage (dry run):**


    python Lightroom_Face_to_Metadata.py --catalog /path/to/catalog.lrcat

    Lightroom_Face_to_Metadata.exe --catalog /path/to/catalog.lrcat

**Write mode with hierarchical keywords:**


    python Lightroom_Face_to_Metadata.py --catalog /path/to/catalog.lrcat --write --tags

    Lightroom_Face_to_Metadata.exe --catalog /path/to/catalog.lrcat --write --tags


**Performance tuning for large catalogs:**


    python Lightroom_Face_to_Metadata.py --catalog /path/to/catalog.lrcat --batch-size 2000
    
    Lightroom_Face_to_Metadata.exe --catalog /path/to/catalog.lrcat --batch-size 2000

**With profiling:**


    python Lightroom_Face_to_Metadata.py --catalog /path/to/catalog.lrcat --profile performance.prof
    
    Lightroom_Face_to_Metadata.exe --catalog /path/to/catalog.lrcat --profile performance.prof
  
**GUI Mode:**


    python Lightroom_Face_to_Metadata.py

    Lightroom_Face_to_Metadata.exe
  



*Copyright (c) 2025, Massimo Savazzi* 
   
*All rights reserved.*
*This script is licensed under the MIT License.*
   
  
*Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions: The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.*
*THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.* 
