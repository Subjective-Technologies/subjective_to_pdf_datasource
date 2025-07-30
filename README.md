# SubjectiveToPdfDataSource

Batch data source for PDF processing and merging - Part of Subjective Technologies Data Source ecosystem.

## Overview

This data source provides batch extraction and processing of PDF files, including merging multiple PDFs into a single file, compression using Ghostscript, and optional chunking by pages or file size. It's designed to handle large collections of PDF documents efficiently.

## Features

- 📄 PDF file discovery and validation
- 🔗 Merge multiple PDFs into a single file
- 🗜️ PDF compression using Ghostscript
- ✂️ PDF chunking by pages or file size
- 🔍 Duplicate detection using SHA256 checksums
- 📝 Comprehensive logging and processing reports
- 💾 Flexible storage system integration
- 🔧 Configurable processing parameters
- 🛡️ Error handling and recovery
- 🐍 Python 3.8+ compatible

## Installation

### Using Conda (Recommended)

```bash
# Create environment from environment.yml
conda env create -f environment.yml

# Activate environment
conda activate to-pdf
```

### Using Pip

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\\Scripts\\activate

# Install dependencies
pip install -r requirements.txt
```

### System Dependencies

For PDF compression functionality, install Ghostscript:

**Ubuntu/Debian:**
```bash
sudo apt-get install ghostscript
```

**macOS:**
```bash
brew install ghostscript
```

**Windows:**
Download from [Ghostscript website](https://www.ghostscript.com/releases/gsdnld.html)

## Configuration

Create a `.env` file in the project root with the following variables:

```env
# Input/Output Configuration
INPUT_DIRECTORY=./input  # Directory containing PDF files to process
OUTPUT_DIRECTORY=./output  # Directory for processed files

# Processing Configuration
ENABLE_COMPRESSION=true  # Enable PDF compression (requires Ghostscript)
CHUNK_CONFIG=3  # Optional: Split into 3 parts by pages
# CHUNK_CONFIG=50MB  # Optional: Split by file size (50MB chunks)
```

## Usage

### Direct Execution

```bash
python SubjectiveToPdfDataSource.py
```

### Programmatic Usage

```python
from SubjectiveToPdfDataSource import SubjectiveToPdfDataSource

# Configuration
config = {
    'storage_config': {
        'type': 'file',
        'path': './output'
    },
    'datasource_config': {
        'input_directory': './input',  # Directory with PDF files
        'output_directory': './output',  # Output directory
        'enable_compression': True,  # Enable compression
        'chunk_config': '3',  # Split into 3 parts by pages
        # 'chunk_config': '50MB',  # Or split by file size
    }
}

# Create and run data source
datasource = SubjectiveToPdfDataSource(config)

# Connect to data source
if datasource.connect():
    # Run batch processing
    success = datasource.run()
    if success:
        print("PDF processing completed successfully")
    else:
        print("PDF processing failed")
else:
    print("Failed to connect to data source")
```

## Processing Workflow

1. **Discovery**: Recursively finds all PDF files in the input directory
2. **Validation**: Checks each PDF for validity and non-zero size
3. **Deduplication**: Removes duplicate files using SHA256 checksums
4. **Merging**: Combines all valid PDFs into a single file
5. **Compression**: Optionally compresses the merged PDF using Ghostscript
6. **Chunking**: Optionally splits the PDF by pages or file size
7. **Logging**: Records all processing steps and results

## Data Structure

The data source extracts and stores data in the following JSON structure:

### PDF File Record
```json
{
  "id": "pdf_a1b2c3d4e5f6",
  "timestamp": "2024-01-01T12:00:00.000Z",
  "source": "to_pdf",
  "file_path": "/path/to/document.pdf",
  "file_name": "document.pdf",
  "file_size": 1024000,
  "file_hash": "a1b2c3d4e5f6...",
  "modified_time": "2024-01-01T10:00:00.000Z",
  "is_valid": true,
  "processed_at": "2024-01-01T12:00:01.000Z",
  "processor": "SubjectiveToPdfDataSource"
}
```

### Processing Result Record
```json
{
  "id": "processing_2024-01-01T12:00:00.000Z",
  "timestamp": "2024-01-01T12:00:00.000Z",
  "source": "to_pdf",
  "input_directory": "/path/to/input",
  "output_file": "/path/to/output/merged.pdf",
  "merge_success": true,
  "compression_enabled": true,
  "compression_success": true,
  "chunk_config": "3",
  "chunk_files": ["merged_part1.pdf", "merged_part2.pdf", "merged_part3.pdf"],
  "total_input_pdfs": 5,
  "processed_at": "2024-01-01T12:00:01.000Z",
  "processor": "SubjectiveToPdfDataSource"
}
```

## Output Structure

The data source creates the following output structure:

```
./output/
├── merged.pdf                    # Merged PDF file
├── merged_part1.pdf             # Chunk files (if chunking enabled)
├── merged_part2.pdf
├── merged_part3.pdf
└── processing_log_20240101_120000.json  # Processing log
```

## Configuration Options

### Chunk Configuration

**By Pages:**
- `"3"` - Split into 3 parts with equal pages
- `"10"` - Split into 10 parts with equal pages

**By File Size:**
- `"50MB"` - Split into 50MB chunks
- `"1GB"` - Split into 1GB chunks

### Compression Settings

The data source uses Ghostscript with the following settings:
- Compatibility Level: 1.4
- PDF Settings: /ebook (optimized for e-books)
- Timeout: 5 minutes per file

## Development

### Adding Custom Logic

1. **PDF Processing**: Modify the `merge_pdfs()`, `compress_pdf()`, or chunking methods
2. **Data Extraction**: Customize the `extract_data()` method for different PDF metadata
3. **Configuration**: Add custom configuration parameters in the `datasource_config` section

### Testing

```bash
# Create test directory with PDF files
mkdir -p input
# Copy some PDF files to input/

# Run the data source
python SubjectiveToPdfDataSource.py

# Check output
ls -la output/
```

### Debug Mode

```bash
# Run with debug logging
export LOG_LEVEL=DEBUG
python SubjectiveToPdfDataSource.py
```

## Troubleshooting

### Common Issues

1. **Ghostscript Not Found**: Install Ghostscript or disable compression
2. **Permission Errors**: Ensure read/write permissions on input/output directories
3. **Invalid PDFs**: Check for corrupted or empty PDF files
4. **Memory Issues**: Use chunking for very large PDF collections

### Logging

The data source provides comprehensive logging. Set the `LOG_LEVEL` environment variable:

```bash
export LOG_LEVEL=DEBUG  # DEBUG, INFO, WARNING, ERROR
```

### Performance Tips

1. **Large Collections**: Use chunking to process large PDF collections
2. **Compression**: Enable compression for storage optimization
3. **Deduplication**: The system automatically removes duplicates
4. **Parallel Processing**: Consider running multiple instances for different directories

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is part of Subjective Technologies and follows the organization's licensing terms.

## Support

For support and questions:
- Create an issue in the repository
- Contact Subjective Technologies support
- Check the documentation at [Subjective Technologies](https://github.com/Subjective-Technologies)

---

Generated by Subjective CLI (subcli) v1.0.0
