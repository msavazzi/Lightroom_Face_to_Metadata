Script to sync Lightroom face data to image metadata using ExifTool
Based on the database schema from: Lightroom Classic 14.3
Analyzed as SQLite 3 Database

Needed as Lightroom does not write face data to metadata nor it provides a way to export it via Plugin API

Options are:
    --catalog           Path to Lightroom .lrcat file
    --log               Path to save the log file - default log.txt in the same folder of the script
    --write             Enable actual writing (default is dry-run)
    --exiftool-path     Path to the exiftool executable - default is exiftool in the same folder of the script
    --log-level         Set the logging level - default is 'INFO', choices are 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'

# This script is intended for educational purposes only. Use at your own risk.  

# It is recommended to backup your Lightroom catalog and images before running this script.

# This script is provided "as is" without warranty of any kind, either expressed or implied.    

# The author is not responsible for any damage or loss of data that may occur as a result of using this script.

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
