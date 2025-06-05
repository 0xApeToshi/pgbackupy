# pgbackupy
[![Build Status](https://github.com/0xApeToshi/pgbackupy/actions/workflows/test.yml/badge.svg)](https://github.com/0xApeToshi/pgbackupy/actions/workflows/test.yml)
[![Python 3.13](https://img.shields.io/badge/python-3.13-blue.svg)](https://python.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A fast, async Python tool to backup all PostgreSQL tables to CSV files with concurrent processing and smart memory management.

## Features

- 🚀 **Async & Fast**: Uses asyncpg for high-performance database operations
- 🔄 **Concurrent Downloads**: Process multiple tables simultaneously
- 💾 **Memory Efficient**: Chunked processing for large tables to prevent memory issues
- 📊 **Smart Analysis**: Analyzes table sizes and row counts before downloading
- 🔧 **Configurable**: Flexible configuration via environment variables
- 📝 **Detailed Logging**: Progress tracking and comprehensive summaries
- 🛡️ **Robust**: Connection pooling, timeouts, and error handling

## Installation

### Requirements
- Python 3.7+
- PostgreSQL database

### Install Dependencies

```bash
pip install -r requirements.txt
```

## Quick Start

1. **Clone or download** the script

2. **Create a `.env` file** in the same directory:
```bash
# Required
DB_NAME=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password

# Optional
DB_HOST=localhost
DB_PORT=5432
DB_SCHEMA=public
OUTPUT_DIR=downloaded_tables
MAX_CONNECTIONS=10
MAX_CONCURRENT_DOWNLOADS=3
```

3. **Run the backup**:
```bash
python pgbackupy.py
```

## Configuration

All configuration is done via environment variables in your `.env` file:

| Variable | Description | Default |
|----------|-------------|---------|
| `DB_HOST` | Database host | `localhost` |
| `DB_NAME` | Database name | **Required** |
| `DB_USER` | Username | **Required** |
| `DB_PASSWORD` | Password | **Required** |
| `DB_PORT` | Port number | `5432` |
| `DB_SCHEMA` | Schema to backup | `public` |
| `OUTPUT_DIR` | Output directory for CSV files | `downloaded_tables` |
| `MAX_CONNECTIONS` | Connection pool size | `10` |
| `MAX_CONCURRENT_DOWNLOADS` | Concurrent table downloads | `3` |

## Usage Examples

### Basic Usage
```bash
python pgbackupy.py
```

### Custom Output Directory
```bash
# In your .env file
OUTPUT_DIR=backups/prod_backup_2024
```

### High Performance Setup
```bash
# In your .env file
MAX_CONNECTIONS=20
MAX_CONCURRENT_DOWNLOADS=5
```

### Backup Specific Schema
```bash
# In your .env file
DB_SCHEMA=analytics
```

## Output

The tool creates CSV files in your specified output directory with timestamps:

```
downloaded_tables/
├── users_20240605_164415.csv
├── orders_20240605_164416.csv
├── products_20240605_164417.csv
└── ...
```

### Sample Output
```
2024-06-05 16:44:15,211 - INFO - Successfully created connection pool for database: mydb
2024-06-05 16:44:15,269 - INFO - Found 11 tables in schema 'public'
2024-06-05 16:44:15,270 - INFO - Analyzing tables...
2024-06-05 16:44:15,300 - INFO - Total: 11 tables, 1,247 rows, 2.3 MB
2024-06-05 16:44:15,301 - INFO - Starting concurrent download of 11 tables (max 3 concurrent)...
2024-06-05 16:44:15,469 - INFO - Downloaded table 'users' to backups/users_20240605_164415.csv (13 rows)
2024-06-05 16:44:15,504 - INFO - Downloaded table 'orders' to backups/orders_20240605_164415.csv (324 rows)
...
2024-06-05 16:44:16,170 - INFO - Download complete: 11/11 tables downloaded successfully

============================================================
DOWNLOAD SUMMARY
============================================================
Total tables: 11
Successfully downloaded: 11
Failed downloads: 0
Output directory: backups

Table Details:
  ✓ users: 13 rows, 24 kB
  ✓ orders: 324 rows, 156 kB
  ✓ products: 89 rows, 67 kB
  ...
```

## Performance Tips

### For Large Databases
- Increase `MAX_CONNECTIONS` (e.g., 20-50)
- Increase `MAX_CONCURRENT_DOWNLOADS` (e.g., 5-10)
- Run during off-peak hours

### For Memory-Constrained Systems
- Decrease `MAX_CONCURRENT_DOWNLOADS` (e.g., 1-2)
- The tool automatically uses chunked processing for large tables

### Network Optimization
- Run on the same network as your database
- Use connection pooling (automatically handled)

## Security

- **Never commit your `.env` file** to version control
- Add `.env` to your `.gitignore`:
```bash
echo ".env" >> .gitignore
```
- Use environment variables or secrets management in production
- Consider using read-only database credentials

## Troubleshooting

### Common Issues

**Connection refused:**
```bash
# Check if PostgreSQL is running
pg_isready -h localhost -p 5432

# Verify connection details in .env
```

**Permission denied:**
```bash
# Ensure your user has SELECT permissions on all tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO your_username;
```

**Out of memory:**
```bash
# Reduce concurrent downloads in .env
MAX_CONCURRENT_DOWNLOADS=1
```

**Timeout errors:**
```bash
# The tool has built-in 5-minute timeouts for large queries
# Consider running during off-peak hours for very large tables
```

## Advanced Usage

### Programmatic Usage

```python
import asyncio
from pgbackupy import AsyncPostgreSQLDownloader

async def backup_database():
    downloader = AsyncPostgreSQLDownloader(
        host='localhost',
        database='mydb',
        username='user',
        password='pass'
    )
    
    await downloader.create_pool()
    results = await downloader.download_all_tables()
    await downloader.close_pool()
    
    return results

# Run the backup
results = asyncio.run(backup_database())
```

### Custom Table Selection

The tool currently backs up all tables in a schema. For custom table selection, modify the `get_all_tables()` method or create a custom table list.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

MIT License - feel free to use this tool in your projects!

## Changelog

### v1.0.0
- Initial release with async support
- Concurrent table downloads
- Chunked processing for large tables
- Comprehensive logging and error handling