"""
SubjectiveToPdfDataSource - Subjective Technologies Data Source
Batch data source for PDF processing and merging
"""

import os
import sys
import hashlib
import shutil
import subprocess
import re
import logging
from math import ceil
from typing import Dict, Any, List, Optional
from datetime import datetime
from PyPDF2 import PdfMerger, PdfReader, PdfWriter
from PyPDF2.errors import EmptyFileError
from subjective_abstract_data_source_package import SubjectiveDataSource

# Increase recursion limit for large PDF processing
sys.setrecursionlimit(10000)


class SubjectiveToPdfDataSource(SubjectiveDataSource):
    """
    SubjectiveToPdfDataSource - Batch data source implementation
    
    This data source handles PDF processing, merging, compression, and chunking operations.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the SubjectiveToPdfDataSource.
        
        Args:
            config (Dict[str, Any]): Configuration dictionary containing:
                - storage_config: Storage system configuration
                - datasource_config: Data source specific configuration
        """
        super().__init__(config)
        self.logger = logging.getLogger(__name__)
        
        # Initialize configuration
        self.storage_config = config.get('storage_config', {})
        self.datasource_config = config.get('datasource_config', {})
        
        # PDF processing configuration
        self.input_directory = self.datasource_config.get('input_directory', '')
        self.output_directory = self.datasource_config.get('output_directory', './output')
        self.enable_compression = self.datasource_config.get('enable_compression', True)
        self.chunk_config = self.datasource_config.get('chunk_config', None)
        
        self.logger.info(f"Initialized SubjectiveToPdfDataSource with config")
    
    def validate_config(self) -> bool:
        """
        Validate the configuration for this data source.
        
        Returns:
            bool: True if configuration is valid, False otherwise
        """
        if not self.input_directory:
            self.logger.error("Input directory configuration is required")
            return False
        
        if not os.path.isdir(self.input_directory):
            self.logger.error(f"Input directory does not exist: {self.input_directory}")
            return False
            
        # Create output directory if it doesn't exist
        os.makedirs(self.output_directory, exist_ok=True)
        
        return True
    
    def connect(self) -> bool:
        """
        Establish connection to the data source.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            if not self.validate_config():
                return False
                
            self.logger.info("Successfully connected to PDF data source")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            return False
    
    # -------------------- PDF UTILITIES -------------------- #
    
    def compute_checksum(self, file_path: str) -> str:
        """Compute SHA256 checksum of a file."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    def find_all_pdfs(self, base_dir: str) -> List[str]:
        """Find all PDF files in a directory recursively."""
        pdf_files = []
        for root, _, files in os.walk(base_dir):
            for f in files:
                if f.lower().endswith(".pdf"):
                    pdf_files.append(os.path.join(root, f))
        return pdf_files
    
    def is_valid_pdf(self, pdf_path: str) -> bool:
        """Check if a PDF file is valid and not empty."""
        if not os.path.isfile(pdf_path) or os.path.getsize(pdf_path) == 0:
            return False
        try:
            PdfReader(pdf_path)
            return True
        except Exception:
            return False
    
    # -------------------- PDF MERGING -------------------- #
    
    def merge_pdfs(self, input_dir: str, output_file: str) -> bool:
        """Merge all PDFs in a directory into a single PDF."""
        pdf_files = self.find_all_pdfs(input_dir)
        pdf_files.sort(key=lambda f: os.path.getmtime(f), reverse=True)

        if not pdf_files:
            self.logger.warning("No PDF files found.")
            return False

        seen_hashes = set()
        merger = PdfMerger()

        for pdf in pdf_files:
            if not self.is_valid_pdf(pdf):
                self.logger.warning(f"Skipped invalid or empty PDF: {pdf}")
                continue

            file_hash = self.compute_checksum(pdf)
            if file_hash in seen_hashes:
                self.logger.info(f"Skipped duplicate: {pdf}")
                continue

            try:
                seen_hashes.add(file_hash)
                with open(pdf, "rb") as f:
                    merger.append(f)
                self.logger.info(f"Added: {pdf}")
            except Exception as e:
                self.logger.error(f"Failed to merge {pdf}: {e}")

        if not seen_hashes:
            self.logger.warning("No valid PDFs to merge.")
            return False

        with open(output_file, "wb") as out_f:
            merger.write(out_f)

        merger.close()
        self.logger.info(f"Merged PDF created: {output_file}")
        return True
    
    # -------------------- PDF COMPRESSION -------------------- #
    
    def compress_pdf(self, input_pdf: str) -> bool:
        """Compress PDF using Ghostscript."""
        gs_path = shutil.which("gs")
        if not gs_path:
            self.logger.warning("Ghostscript not found, skipping compression.")
            return False

        temp_output = input_pdf + ".tmp"
        gs_command = [
            gs_path, "-sDEVICE=pdfwrite", "-dCompatibilityLevel=1.4",
            "-dPDFSETTINGS=/ebook", "-dNOPAUSE", "-dQUIET", "-dBATCH",
            f"-sOutputFile={temp_output}", input_pdf
        ]

        try:
            subprocess.run(gs_command, check=True, timeout=300)  # 5 min timeout
            os.replace(temp_output, input_pdf)
            self.logger.info(f"Compressed PDF: {input_pdf}")
            return True
        except subprocess.TimeoutExpired:
            self.logger.warning("Ghostscript compression timed out. Keeping uncompressed file.")
            return False
        except subprocess.CalledProcessError:
            self.logger.warning("Ghostscript compression failed, original kept.")
            return False
        except Exception as e:
            self.logger.error(f"Compression error: {e}")
            return False
    
    # -------------------- PDF CHUNKING -------------------- #
    
    def parse_chunk_arg(self, arg: str) -> Optional[tuple]:
        """Parse chunk argument: either N pages or SIZE (MB/GB)."""
        size_match = re.match(r"(\d+)(MB|GB)", arg.upper())
        if size_match:
            size = int(size_match.group(1))
            unit = size_match.group(2)
            return ("size", size * (1024**2 if unit == "MB" else 1024**3))
        elif arg.isdigit():
            return ("pages", int(arg))
        else:
            return None
    
    def split_pdf_by_pages(self, input_pdf: str, chunks: int) -> List[str]:
        """Split PDF into chunks by number of pages using external tools for reliability."""
        try:
            # First try to get page count using PyPDF2
            reader = PdfReader(input_pdf)
            total_pages = len(reader.pages)
            pages_per_chunk = ceil(total_pages / chunks)
            
            base_name = os.path.splitext(input_pdf)[0]
            output_files = []
            
            self.logger.info(f"Splitting PDF: {total_pages} pages into {chunks} chunks ({pages_per_chunk} pages each)")
            
            # Try using pdftk first (most reliable for large files)
            pdftk_path = shutil.which("pdftk")
            if pdftk_path:
                self.logger.info("Using pdftk for chunking (most reliable for large files)")
                return self._split_pdf_with_pdftk(input_pdf, total_pages, chunks, pages_per_chunk, base_name)
            
            # Fallback to ghostscript
            gs_path = shutil.which("gs")
            if gs_path:
                self.logger.info("Using Ghostscript for chunking")
                return self._split_pdf_with_ghostscript(input_pdf, total_pages, chunks, pages_per_chunk, base_name)
            
            # Last resort: PyPDF2 with memory management
            self.logger.info("Using PyPDF2 with memory management (may be slow for large files)")
            return self._split_pdf_with_pypdf2_fallback(input_pdf, total_pages, chunks, pages_per_chunk, base_name)
            
        except Exception as e:
            self.logger.error(f"❌ Error in split_pdf_by_pages: {e}")
            return []
    
    def _split_pdf_with_pdftk(self, input_pdf: str, total_pages: int, chunks: int, pages_per_chunk: int, base_name: str) -> List[str]:
        """Split PDF using pdftk (most reliable for large files)."""
        output_files = []
        pdftk_path = shutil.which("pdftk")
        
        for i in range(chunks):
            try:
                start = i * pages_per_chunk + 1  # pdftk uses 1-based page numbers
                end = min((i + 1) * pages_per_chunk, total_pages)
                
                self.logger.info(f"Creating chunk {i+1}: pages {start} to {end}")
                
                output_name = f"{base_name}_part{i+1}.pdf"
                cmd = [pdftk_path, input_pdf, "cat", f"{start}-{end}", "output", output_name]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                
                if result.returncode == 0:
                    output_files.append(output_name)
                    file_size = os.path.getsize(output_name)
                    self.logger.info(f"✅ Created chunk: {output_name} ({end - start + 1} pages, {file_size:,} bytes)")
                else:
                    self.logger.error(f"❌ pdftk failed for chunk {i+1}: {result.stderr}")
                    
            except Exception as e:
                self.logger.error(f"❌ Failed to create chunk {i+1} with pdftk: {e}")
                continue
        
        return output_files
    
    def _split_pdf_with_ghostscript(self, input_pdf: str, total_pages: int, chunks: int, pages_per_chunk: int, base_name: str) -> List[str]:
        """Split PDF using Ghostscript."""
        output_files = []
        
        for i in range(chunks):
            try:
                start = i * pages_per_chunk + 1
                end = min((i + 1) * pages_per_chunk, total_pages)
                
                self.logger.info(f"Creating chunk {i+1}: pages {start} to {end}")
                
                output_name = f"{base_name}_part{i+1}.pdf"
                
                # Ghostscript command for page extraction
                cmd = [
                    "gs", "-sDEVICE=pdfwrite", "-dNOPAUSE", "-dQUIET", "-dBATCH",
                    f"-sOutputFile={output_name}",
                    "-f", input_pdf,
                    "-c", f"<< /PageRange [{start} {end}] >> setpagedevice",
                    "-c", "showpage"
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
                
                if result.returncode == 0 and os.path.exists(output_name):
                    output_files.append(output_name)
                    file_size = os.path.getsize(output_name)
                    self.logger.info(f"✅ Created chunk: {output_name} ({end - start + 1} pages, {file_size:,} bytes)")
                else:
                    self.logger.error(f"❌ Ghostscript failed for chunk {i+1}: {result.stderr}")
                    
            except Exception as e:
                self.logger.error(f"❌ Failed to create chunk {i+1} with Ghostscript: {e}")
                continue
        
        return output_files
    
    def _split_pdf_with_pypdf2_fallback(self, input_pdf: str, total_pages: int, chunks: int, pages_per_chunk: int, base_name: str) -> List[str]:
        """Fallback to PyPDF2 with improved memory management."""
        output_files = []
        
        # Read the PDF once and store page references
        reader = PdfReader(input_pdf)
        
        for i in range(chunks):
            try:
                start = i * pages_per_chunk
                end = min(start + pages_per_chunk, total_pages)
                
                self.logger.info(f"Creating chunk {i+1}: pages {start} to {end-1}")
                
                # Create a new PdfWriter for each chunk
                writer = PdfWriter()
                
                # Add pages in smaller batches to avoid recursion issues
                batch_size = 50  # Smaller batch size for better memory management
                for batch_start in range(start, end, batch_size):
                    batch_end = min(batch_start + batch_size, end)
                    
                    # Progress update
                    if batch_start % 500 == 0 or batch_start == start:
                        self.logger.info(f"  Processing pages {batch_start} to {batch_end-1} for chunk {i+1}")
                    
                    # Add pages in this batch
                    for p in range(batch_start, batch_end):
                        try:
                            writer.add_page(reader.pages[p])
                        except Exception as e:
                            self.logger.warning(f"Failed to add page {p}: {e}")
                            continue
                
                # Write the chunk
                output_name = f"{base_name}_part{i+1}.pdf"
                with open(output_name, "wb") as f:
                    writer.write(f)
                
                output_files.append(output_name)
                file_size = os.path.getsize(output_name)
                self.logger.info(f"✅ Created chunk: {output_name} ({end - start} pages, {file_size:,} bytes)")
                
                # Clear the writer to free memory
                del writer
                
            except Exception as e:
                self.logger.error(f"❌ Failed to create chunk {i+1}: {e}")
                # Continue with next chunk instead of failing completely
                continue
        
        return output_files
    
    def split_pdf_by_size(self, input_pdf: str, max_size: int) -> List[str]:
        """Split PDF into chunks by file size."""
        try:
            reader = PdfReader(input_pdf)
            base_name = os.path.splitext(input_pdf)[0]
            writer = PdfWriter()
            part = 1
            output_files = []

            for page in reader.pages:
                writer.add_page(page)
                temp_file = f"{base_name}_temp.pdf"
                with open(temp_file, "wb") as f:
                    writer.write(f)
                size = os.path.getsize(temp_file)

                if size >= max_size:
                    output_name = f"{base_name}_part{part}.pdf"
                    os.replace(temp_file, output_name)
                    output_files.append(output_name)
                    self.logger.info(f"Created chunk: {output_name}")
                    writer = PdfWriter()
                    part += 1

            if len(writer.pages) > 0:
                output_name = f"{base_name}_part{part}.pdf"
                with open(output_name, "wb") as f:
                    writer.write(f)
                output_files.append(output_name)
                self.logger.info(f"Created chunk: {output_name}")

            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            return output_files
        except Exception as e:
            self.logger.error(f"❌ Error in split_pdf_by_size: {e}")
            return []
    
    # -------------------- DATA SOURCE METHODS -------------------- #
    
    def extract_data(self) -> List[Dict[str, Any]]:
        """
        Extract PDF data from the input directory.
        
        Returns:
            List[Dict[str, Any]]: List of extracted PDF data records
        """
        try:
            self.logger.info("Starting PDF data extraction")
            
            # Find all PDFs in input directory
            pdf_files = self.find_all_pdfs(self.input_directory)
            
            extracted_data = []
            
            for pdf_file in pdf_files:
                if self.is_valid_pdf(pdf_file):
                    file_stat = os.stat(pdf_file)
                    file_hash = self.compute_checksum(pdf_file)
                    
                    pdf_data = {
                        'id': f'pdf_{file_hash[:16]}',
                        'timestamp': datetime.now().isoformat(),
                        'source': 'to_pdf',
                        'file_path': pdf_file,
                        'file_name': os.path.basename(pdf_file),
                        'file_size': file_stat.st_size,
                        'file_hash': file_hash,
                        'modified_time': datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
                        'is_valid': True
                    }
                    
                    extracted_data.append(pdf_data)
                else:
                    self.logger.warning(f"Invalid PDF file: {pdf_file}")
            
            self.logger.info(f"Extracted {len(extracted_data)} PDF records")
            return extracted_data
            
        except Exception as e:
            self.logger.error(f"Error extracting PDF data: {e}")
            return []
    
    def process_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process the extracted PDF data by merging, compressing, and chunking.
        
        Args:
            data (List[Dict[str, Any]]): Raw extracted PDF data
            
        Returns:
            List[Dict[str, Any]]: Processed data with operation results
        """
        try:
            self.logger.info(f"Processing {len(data)} PDF records")
            
            processed_data = []
            
            # Generate output filename
            folder_name = os.path.basename(os.path.normpath(self.input_directory))
            output_file = os.path.join(self.output_directory, folder_name + ".pdf")
            
            # Merge PDFs
            merge_success = self.merge_pdfs(self.input_directory, output_file)
            
            if merge_success:
                # Compress if enabled
                compression_success = False
                if self.enable_compression:
                    compression_success = self.compress_pdf(output_file)
                
                # Chunk if configured
                chunk_files = []
                if self.chunk_config:
                    chunk_mode = self.parse_chunk_arg(self.chunk_config)
                    if chunk_mode:
                        if chunk_mode[0] == "pages":
                            chunk_files = self.split_pdf_by_pages(output_file, chunk_mode[1])
                        else:
                            chunk_files = self.split_pdf_by_size(output_file, chunk_mode[1])
                    else:
                        self.logger.warning(f"Invalid chunk configuration: {self.chunk_config}")
                
                # Create processing result
                processing_result = {
                    'id': f'processing_{datetime.now().isoformat()}',
                    'timestamp': datetime.now().isoformat(),
                    'source': 'to_pdf',
                    'input_directory': self.input_directory,
                    'output_file': output_file,
                    'merge_success': merge_success,
                    'compression_enabled': self.enable_compression,
                    'compression_success': compression_success,
                    'chunk_config': self.chunk_config,
                    'chunk_files': chunk_files,
                    'total_input_pdfs': len(data),
                    'processed_at': datetime.now().isoformat(),
                    'processor': 'SubjectiveToPdfDataSource'
                }
                
                processed_data.append(processing_result)
                
                # Add individual PDF records
                for pdf_record in data:
                    processed_record = {
                        **pdf_record,
                        'processed_at': datetime.now().isoformat(),
                        'processor': 'SubjectiveToPdfDataSource'
                    }
                    processed_data.append(processed_record)
            
            self.logger.info(f"Processed {len(processed_data)} records")
            return processed_data
            
        except Exception as e:
            self.logger.error(f"Error processing PDF data: {e}")
            return data  # Return original data if processing fails
    
    def store_data(self, data: List[Dict[str, Any]]) -> bool:
        """
        Store the processed data to the configured storage system.
        
        Args:
            data (List[Dict[str, Any]]): Processed data to store
            
        Returns:
            bool: True if storage successful, False otherwise
        """
        try:
            if not data:
                self.logger.warning("No data to store")
                return True
            
            # Store to output directory as JSON for now
            # In a real implementation, you might store to a database or other storage system
            output_file = os.path.join(self.output_directory, f"processing_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            
            # Limit data to avoid JSON recursion errors with large datasets
            # Store only processing results and first 100 PDF records
            limited_data = []
            processing_results = []
            pdf_records = []
            
            for record in data:
                if record.get('id', '').startswith('processing_'):
                    processing_results.append(record)
                else:
                    pdf_records.append(record)
            
            # Add all processing results
            limited_data.extend(processing_results)
            
            # Add summary info about PDF records instead of all records
            if pdf_records:
                summary_record = {
                    'id': f'summary_{datetime.now().isoformat()}',
                    'timestamp': datetime.now().isoformat(),
                    'source': 'to_pdf',
                    'total_pdf_files': len(pdf_records),
                    'sample_files': pdf_records[:10],  # First 10 as samples
                    'summary_stats': {
                        'total_files': len(pdf_records),
                        'total_size_bytes': sum(r.get('file_size', 0) for r in pdf_records),
                        'valid_files': sum(1 for r in pdf_records if r.get('is_valid', False)),
                        'invalid_files': sum(1 for r in pdf_records if not r.get('is_valid', True))
                    }
                }
                limited_data.append(summary_record)
            
            import json
            with open(output_file, 'w') as f:
                json.dump(limited_data, f, indent=2, default=str)
            
            self.logger.info(f"Successfully stored {len(limited_data)} records to {output_file}")
            self.logger.info(f"Processing results: {len(processing_results)}, PDF summary: {len(pdf_records)} files")
            return True
            
        except Exception as e:
            self.logger.error(f"Error storing data: {e}")
            return False
    
    def run(self) -> bool:
        """
        Run the PDF processing data source.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not self.connect():
                return False
            
            self.logger.info("Starting PDF processing")
            
            # Extract data
            data = self.extract_data()
            
            if data:
                # Process data
                processed_data = self.process_data(data)
                
                # Store data
                if self.store_data(processed_data):
                    self.logger.info("PDF processing completed successfully")
                    return True
                else:
                    self.logger.error("Failed to store data")
                    return False
            else:
                self.logger.info("No PDF files found to process")
                return True
            
        except Exception as e:
            self.logger.error(f"Error in PDF processing: {e}")
            return False
    
    def disconnect(self) -> None:
        """
        Clean up and disconnect from the data source.
        """
        try:
            self.logger.info("Disconnected from PDF data source")
        except Exception as e:
            self.logger.error(f"Error during disconnect: {e}")
    
    def get_icon(self) -> str:
        """
        Get the SVG icon content for this data source.
        
        Returns:
            str: SVG icon content if icon.svg exists, otherwise a fallback string
        """
        try:
            # Get the directory where this class file is located
            current_dir = os.path.dirname(os.path.abspath(__file__))
            icon_path = os.path.join(current_dir, "icon.svg")
            
            if os.path.exists(icon_path):
                with open(icon_path, 'r', encoding='utf-8') as f:
                    return f.read()
            else:
                # Fallback SVG icon for PDF processing
                return '''<?xml version="1.0" encoding="UTF-8"?>
<svg width="64" height="64" viewBox="0 0 64 64" xmlns="http://www.w3.org/2000/svg">
  <defs>
    <linearGradient id="gradient" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%" style="stop-color:#4ECDC4;stop-opacity:1" />
      <stop offset="100%" style="stop-color:#4ECDC480;stop-opacity:1" />
    </linearGradient>
  </defs>
  
  <!-- Background circle -->
  <circle cx="32" cy="32" r="30" fill="url(#gradient)" stroke="#333" stroke-width="2"/>
  
  <!-- PDF icon symbol -->
  <text x="32" y="42" font-family="Arial, sans-serif" font-size="24" text-anchor="middle" fill="white">
    📄
  </text>
  
  <!-- Data source type indicator -->
  <text x="32" y="56" font-family="Arial, sans-serif" font-size="8" text-anchor="middle" fill="#333">
    BATCH
  </text>
  
  <!-- Corner indicator for Subjective Technologies -->
  <circle cx="52" cy="12" r="8" fill="#333" opacity="0.8"/>
  <text x="52" y="16" font-family="Arial, sans-serif" font-size="8" text-anchor="middle" fill="white">S</text>
</svg>'''
                
        except Exception as e:
            self.logger.error(f"Error reading icon file: {e}")
            # Return a simple fallback string
            return "📄 PDF Data Source"
    
    def get_connection_metadata(self) -> Dict[str, Any]:
        """
        Get connection metadata for interactive parameter collection.
        
        Returns:
            Dict[str, Any]: Dictionary describing required connection parameters
        """
        return {
            'input_directory': {
                'description': 'Directory containing PDF files to process',
                'type': 'string',
                'required': True,
                'default': './input',
                'sensitive': False
            },
            'output_directory': {
                'description': 'Directory where processed files will be saved',
                'type': 'string',
                'required': False,
                'default': './output',
                'sensitive': False
            },
            'enable_compression': {
                'description': 'Enable PDF compression using Ghostscript (requires Ghostscript installed)',
                'type': 'bool',
                'required': False,
                'default': True,
                'sensitive': False
            },
            'chunk_config': {
                'description': 'Optional chunking configuration (e.g., "3" for 3 parts by pages, "50MB" for 50MB chunks)',
                'type': 'string',
                'required': False,
                'default': None,
                'sensitive': False
            }
        }
    
    def fetch(self) -> List[Dict[str, Any]]:
        """
        Fetch data from the data source.
        
        Returns:
            List[Dict[str, Any]]: List of fetched data records
        """
        return self.extract_data()
    
    def get_connection_data(self) -> Dict[str, Any]:
        """
        Get connection data for the data source.
        
        Returns:
            Dict[str, Any]: Connection data dictionary
        """
        return {
            'input_directory': self.input_directory,
            'output_directory': self.output_directory,
            'enable_compression': self.enable_compression,
            'chunk_config': self.chunk_config
        }


def main():
    """
    Main entry point for running the SubjectiveToPdfDataSource directly.
    """
    import json
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Configuration
    config = {
        'storage_config': {
            'type': 'file',
            'path': './output'
        },
        'datasource_config': {
            'input_directory': os.getenv('INPUT_DIRECTORY', './input'),
            'output_directory': os.getenv('OUTPUT_DIRECTORY', './output'),
            'enable_compression': os.getenv('ENABLE_COMPRESSION', 'true').lower() == 'true',
            'chunk_config': os.getenv('CHUNK_CONFIG', None),  # e.g., "3" for pages or "50MB" for size
        }
    }
    
    # Create and run data source
    datasource = SubjectiveToPdfDataSource(config)
    
    try:
        success = datasource.run()
        if not success:
            sys.exit(1)
    finally:
        datasource.disconnect()


if __name__ == "__main__":
    main()
