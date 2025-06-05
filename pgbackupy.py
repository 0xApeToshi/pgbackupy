import asyncpg
import asyncio
import pandas as pd
import os
from datetime import datetime
import logging
from dotenv import load_dotenv
from typing import List, Dict

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AsyncPostgreSQLDownloader:
    def __init__(self, host: str, database: str, username: str, password: str, port: int = 5432, max_connections: int = 10):
        """
        Initialize AsyncPostgreSQL connection parameters
        
        Args:
            host (str): Database host
            database (str): Database name
            username (str): Username
            password (str): Password
            port (int): Port number (default: 5432)
            max_connections (int): Maximum concurrent connections (default: 10)
        """
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.max_connections = max_connections
        self.connection_pool = None
        
    async def create_pool(self):
        """Create connection pool"""
        try:
            self.connection_pool = await asyncpg.create_pool(
                host=self.host,
                database=self.database,
                user=self.username,
                password=self.password,
                port=self.port,
                min_size=1,
                max_size=self.max_connections,
                command_timeout=300,  # 5 minutes timeout for large queries
                server_settings={
                    'application_name': 'postgres_table_downloader',
                }
            )
            logger.info(f"Successfully created connection pool for database: {self.database}")
            return True
        except Exception as e:
            logger.error(f"Error creating connection pool: {e}")
            return False
    
    async def get_all_tables(self, schema: str = 'public') -> List[str]:
        """
        Get list of all tables in the specified schema
        
        Args:
            schema (str): Schema name (default: 'public')
            
        Returns:
            List[str]: List of table names
        """
        try:
            async with self.connection_pool.acquire() as connection:
                query = """
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = $1 AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """
                rows = await connection.fetch(query, schema)
                tables = [row['table_name'] for row in rows]
                logger.info(f"Found {len(tables)} tables in schema '{schema}'")
                return tables
        except Exception as e:
            logger.error(f"Error getting table list: {e}")
            return []
    
    async def get_table_info(self, table_name: str, schema: str = 'public') -> Dict:
        """Get table information including row count and size"""
        try:
            async with self.connection_pool.acquire() as connection:
                # Get row count
                count_query = f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
                row_count = await connection.fetchval(count_query)
                
                # Get table size
                size_query = """
                    SELECT pg_size_pretty(pg_total_relation_size(to_regclass($1))) as size,
                           pg_total_relation_size(to_regclass($1)) as size_bytes
                """
                size_result = await connection.fetchrow(size_query, f'{schema}.{table_name}')
                
                return {
                    'table_name': table_name,
                    'row_count': row_count,
                    'size': size_result['size'],
                    'size_bytes': size_result['size_bytes']
                }
        except Exception as e:
            logger.error(f"Error getting info for table '{table_name}': {e}")
            return {
                'table_name': table_name,
                'row_count': 0,
                'size': 'Unknown',
                'size_bytes': 0
            }
    
    async def download_table_to_csv(self, table_name: str, output_dir: str = 'downloaded_tables', 
                                schema: str = 'public', chunk_size: int = 10000) -> bool:
        """
        Download a specific table to CSV file with chunked processing for large tables
        
        Args:
            table_name (str): Name of the table to download
            output_dir (str): Output directory for CSV files
            schema (str): Schema name (default: 'public')
            chunk_size (int): Number of rows to process at once for large tables
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{table_name}_{timestamp}.csv"
            filepath = os.path.join(output_dir, filename)
            
            async with self.connection_pool.acquire() as connection:
                # First, check if table exists by trying to get its columns
                try:
                    columns_query = """
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_schema = $1 AND table_name = $2
                        ORDER BY ordinal_position
                    """
                    columns = await connection.fetch(columns_query, schema, table_name)
                    
                    if not columns:
                        # Table doesn't exist
                        logger.error(f"Table '{schema}.{table_name}' does not exist")
                        return False
                        
                    column_names = [col['column_name'] for col in columns]
                    
                except Exception as e:
                    logger.error(f"Error checking table existence for '{table_name}': {e}")
                    return False
                
                # Get table info (row count, etc.)
                table_info = await self.get_table_info(table_name, schema)
                row_count = table_info['row_count']
                
                if row_count == 0:
                    # Create empty CSV for tables with no data (but table exists)
                    df = pd.DataFrame(columns=column_names)
                    df.to_csv(filepath, index=False)
                    logger.info(f"Downloaded empty table '{table_name}' to {filepath}")
                    return True
                
                # For large tables, use chunked processing
                if row_count > chunk_size:
                    logger.info(f"Large table detected ({row_count:,} rows). Using chunked processing...")
                    return await self._download_large_table_chunked(
                        connection, table_name, schema, filepath, chunk_size, row_count
                    )
                else:
                    # Small table - download all at once
                    query = f'SELECT * FROM "{schema}"."{table_name}"'
                    rows = await connection.fetch(query)
                    
                    if rows:
                        # Convert to pandas DataFrame
                        df = pd.DataFrame([dict(row) for row in rows])
                        df.to_csv(filepath, index=False)
                    else:
                        # Empty result (shouldn't happen since row_count > 0, but just in case)
                        df = pd.DataFrame(columns=column_names)
                        df.to_csv(filepath, index=False)
                    
                    logger.info(f"Downloaded table '{table_name}' to {filepath} ({row_count:,} rows)")
                    return True
                    
        except Exception as e:
            logger.error(f"Error downloading table '{table_name}': {e}")
            return False
    
    async def _download_large_table_chunked(self, connection, table_name: str, schema: str, 
                                          filepath: str, chunk_size: int, total_rows: int) -> bool:
        """Download large table in chunks to manage memory"""
        try:
            offset = 0
            first_chunk = True
            
            while offset < total_rows:
                query = f'SELECT * FROM "{schema}"."{table_name}" LIMIT $1 OFFSET $2'
                rows = await connection.fetch(query, chunk_size, offset)
                
                if not rows:
                    break
                
                # Convert to DataFrame
                df = pd.DataFrame([dict(row) for row in rows])
                
                # Write to CSV (append mode after first chunk)
                if first_chunk:
                    df.to_csv(filepath, index=False, mode='w')
                    first_chunk = False
                else:
                    df.to_csv(filepath, index=False, mode='a', header=False)
                
                offset += len(rows)
                progress = (offset / total_rows) * 100
                logger.info(f"Progress for '{table_name}': {offset:,}/{total_rows:,} rows ({progress:.1f}%)")
            
            logger.info(f"Downloaded large table '{table_name}' to {filepath} ({total_rows:,} rows)")
            return True
            
        except Exception as e:
            logger.error(f"Error in chunked download for '{table_name}': {e}")
            return False
    
    async def download_all_tables(self, output_dir: str = 'downloaded_tables', 
                                schema: str = 'public', max_concurrent: int = 3) -> Dict:
        """
        Download all tables from the database with concurrent processing
        
        Args:
            output_dir (str): Output directory for CSV files
            schema (str): Schema name (default: 'public')
            max_concurrent (int): Maximum concurrent table downloads
            
        Returns:
            Dict: Summary of download results
        """
        if not self.connection_pool:
            logger.error("No connection pool. Please create pool first.")
            return None
        
        tables = await self.get_all_tables(schema)
        if not tables:
            logger.warning("No tables found to download")
            return None
        
        # Get table info for all tables first
        logger.info("Analyzing tables...")
        table_infos = await asyncio.gather(*[
            self.get_table_info(table, schema) for table in tables
        ])
        
        # Sort tables by size (smallest first to balance load)
        table_infos.sort(key=lambda x: x['size_bytes'])
        
        # Display table summary
        total_size = sum(info['size_bytes'] for info in table_infos)
        total_rows = sum(info['row_count'] for info in table_infos)
        logger.info(f"Total: {len(tables)} tables, {total_rows:,} rows, {self._format_bytes(total_size)}")
        
        results = {
            'total_tables': len(tables),
            'successful_downloads': 0,
            'failed_downloads': 0,
            'downloaded_tables': [],
            'failed_tables': [],
            'table_info': table_infos
        }
        
        logger.info(f"Starting concurrent download of {len(tables)} tables (max {max_concurrent} concurrent)...")
        
        # Create semaphore to limit concurrent downloads
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def download_with_semaphore(table_info):
            async with semaphore:
                success = await self.download_table_to_csv(
                    table_info['table_name'], output_dir, schema
                )
                return table_info['table_name'], success
        
        # Execute downloads concurrently
        download_results = await asyncio.gather(*[
            download_with_semaphore(info) for info in table_infos
        ], return_exceptions=True)
        
        # Process results
        for result in download_results:
            if isinstance(result, Exception):
                logger.error(f"Download task failed: {result}")
                results['failed_downloads'] += 1
            else:
                table_name, success = result
                if success:
                    results['successful_downloads'] += 1
                    results['downloaded_tables'].append(table_name)
                else:
                    results['failed_downloads'] += 1
                    results['failed_tables'].append(table_name)
        
        logger.info(f"Download complete: {results['successful_downloads']}/{results['total_tables']} tables downloaded successfully")
        
        if results['failed_tables']:
            logger.warning(f"Failed to download: {', '.join(results['failed_tables'])}")
        
        return results
    
    def _format_bytes(self, bytes_value: int) -> str:
        """Format bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"
    
    async def close_pool(self):
        """Close the connection pool"""
        if self.connection_pool:
            await self.connection_pool.close()
            logger.info("Connection pool closed")

async def main():
    """
    Main async function - loads configuration from .env file
    """
    # Load environment variables from .env file
    load_dotenv()
    
    # Database connection parameters from environment variables
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME'),
        'username': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'max_connections': int(os.getenv('MAX_CONNECTIONS', 10))
    }
    
    # Validate required environment variables
    required_vars = ['DB_NAME', 'DB_USER', 'DB_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please check your .env file")
        return
    
    # Configuration from environment variables with defaults
    OUTPUT_DIR = os.getenv('OUTPUT_DIR', 'downloaded_tables')
    SCHEMA = os.getenv('DB_SCHEMA', 'public')
    MAX_CONCURRENT = int(os.getenv('MAX_CONCURRENT_DOWNLOADS', 3))
    
    # Create downloader instance
    downloader = AsyncPostgreSQLDownloader(**DB_CONFIG)
    
    try:
        # Create connection pool
        if await downloader.create_pool():
            # Download all tables
            results = await downloader.download_all_tables(OUTPUT_DIR, SCHEMA, MAX_CONCURRENT)
            
            if results:
                print("\n" + "="*60)
                print("DOWNLOAD SUMMARY")
                print("="*60)
                print(f"Total tables: {results['total_tables']}")
                print(f"Successfully downloaded: {results['successful_downloads']}")
                print(f"Failed downloads: {results['failed_downloads']}")
                print(f"Output directory: {OUTPUT_DIR}")
                
                # Show table details
                if results['table_info']:
                    print(f"\nTable Details:")
                    for info in results['table_info']:
                        status = "✓" if info['table_name'] in results['downloaded_tables'] else "✗"
                        print(f"  {status} {info['table_name']}: {info['row_count']:,} rows, {info['size']}")
                
                if results['failed_tables']:
                    print(f"\nFailed tables:")
                    for table in results['failed_tables']:
                        print(f"  - {table}")
    
    except KeyboardInterrupt:
        logger.info("Download interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        # Always close the connection pool
        await downloader.close_pool()

if __name__ == "__main__":
    asyncio.run(main())