import pytest
import pytest_asyncio
import os
import pandas as pd
from unittest.mock import patch
import tempfile
import shutil
from pathlib import Path

# Import the main class - based on file structure
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pgbackupy import AsyncPostgreSQLDownloader

@pytest.fixture
def test_config():
    """Test database configuration"""
    return {
        'host': 'localhost',
        'database': 'testdb',
        'username': 'testuser',
        'password': 'testpass',
        'port': 5432
    }

@pytest.fixture
def temp_output_dir():
    """Create temporary directory for test outputs"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest_asyncio.fixture
async def downloader(test_config):
    """Create downloader instance for tests"""
    downloader = AsyncPostgreSQLDownloader(**test_config)
    await downloader.create_pool()
    yield downloader
    await downloader.close_pool()

class TestAsyncPostgreSQLDownloader:
    
    @pytest.mark.asyncio
    async def test_connection_pool_creation(self, test_config):
        """Test that connection pool is created successfully"""
        downloader = AsyncPostgreSQLDownloader(**test_config)
        result = await downloader.create_pool()
        assert result is True
        assert downloader.connection_pool is not None
        await downloader.close_pool()
    
    @pytest.mark.asyncio
    async def test_invalid_connection(self):
        """Test connection with invalid credentials"""
        downloader = AsyncPostgreSQLDownloader(
            host='localhost',
            database='nonexistent',
            username='invalid',
            password='invalid'
        )
        result = await downloader.create_pool()
        assert result is False
        assert downloader.connection_pool is None
    
    @pytest.mark.asyncio
    async def test_get_all_tables(self, downloader):
        """Test retrieving all tables from database"""
        tables = await downloader.get_all_tables()
        
        # We expect at least the tables we created in CI
        expected_tables = {'users', 'orders', 'products'}
        found_tables = set(tables)
        
        assert expected_tables.issubset(found_tables), f"Expected tables {expected_tables} not found in {found_tables}"
        assert len(tables) >= 3
    
    @pytest.mark.asyncio
    async def test_get_table_info(self, downloader):
        """Test getting table information"""
        info = await downloader.get_table_info('users')
        
        assert info['table_name'] == 'users'
        assert info['row_count'] >= 0
        assert 'size' in info
        assert 'size_bytes' in info
        assert info['size_bytes'] >= 0
    
    @pytest.mark.asyncio
    async def test_download_single_table(self, downloader, temp_output_dir):
        """Test downloading a single table"""
        result = await downloader.download_table_to_csv('users', temp_output_dir)
        assert result is True
        
        # Check that CSV file was created
        csv_files = list(Path(temp_output_dir).glob('users_*.csv'))
        assert len(csv_files) == 1
        
        # Verify CSV content
        df = pd.read_csv(csv_files[0])
        assert 'id' in df.columns
        assert 'name' in df.columns
        assert 'email' in df.columns
        # Should have the test data we inserted
        assert len(df) >= 3
    
    @pytest.mark.asyncio
    async def test_download_empty_table(self, downloader, temp_output_dir):
        """Test downloading a table with no data"""
        # Create an empty table first
        async with downloader.connection_pool.acquire() as connection:
            await connection.execute("""
                CREATE TABLE IF NOT EXISTS empty_test_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100)
                )
            """)
        
        result = await downloader.download_table_to_csv('empty_test_table', temp_output_dir)
        assert result is True
        
        # Check that CSV file was created
        csv_files = list(Path(temp_output_dir).glob('empty_test_table_*.csv'))
        assert len(csv_files) == 1
        
        # Verify CSV is empty but has headers
        df = pd.read_csv(csv_files[0])
        assert len(df) == 0
        assert 'id' in df.columns
        assert 'name' in df.columns
    
    @pytest.mark.asyncio
    async def test_download_all_tables(self, downloader, temp_output_dir):
        """Test downloading all tables"""
        results = await downloader.download_all_tables(temp_output_dir, max_concurrent=2)
        
        assert results is not None
        assert results['total_tables'] >= 3
        assert results['successful_downloads'] >= 3
        assert results['failed_downloads'] == 0
        
        # Check that CSV files were created for our test tables
        expected_tables = ['users', 'orders', 'products']
        for table in expected_tables:
            csv_files = list(Path(temp_output_dir).glob(f'{table}_*.csv'))
            assert len(csv_files) == 1, f"No CSV file found for table {table}"
    
    @pytest.mark.asyncio
    async def test_chunked_download_simulation(self, downloader, temp_output_dir):
        """Test chunked download by forcing small chunk size"""
        # Use a very small chunk size to force chunking even with small test data
        result = await downloader.download_table_to_csv(
            'orders', temp_output_dir, chunk_size=1
        )
        assert result is True
        
        csv_files = list(Path(temp_output_dir).glob('orders_*.csv'))
        assert len(csv_files) == 1
        
        # Verify all data was downloaded despite chunking
        df = pd.read_csv(csv_files[0])
        assert len(df) >= 4  # We inserted 4 orders in CI setup
    
    @pytest.mark.asyncio
    async def test_nonexistent_table(self, downloader, temp_output_dir):
        """Test handling of non-existent table"""
        result = await downloader.download_table_to_csv('nonexistent_table', temp_output_dir)
        assert result is False
    
    def test_format_bytes(self):
        """Test byte formatting utility"""
        downloader = AsyncPostgreSQLDownloader('', '', '', '')
        
        assert downloader._format_bytes(500) == "500.0 B"
        assert downloader._format_bytes(1500) == "1.5 KB"
        assert downloader._format_bytes(1500000) == "1.4 MB"
        assert downloader._format_bytes(1500000000) == "1.4 GB"

class TestIntegration:
    """Integration tests that test the full workflow"""
    
    @pytest.mark.asyncio
    async def test_full_backup_workflow(self, test_config, temp_output_dir):
        """Test the complete backup workflow"""
        downloader = AsyncPostgreSQLDownloader(**test_config)
        
        # Full workflow test
        assert await downloader.create_pool() is True
        
        tables = await downloader.get_all_tables()
        assert len(tables) >= 3
        
        results = await downloader.download_all_tables(temp_output_dir)
        assert results['successful_downloads'] >= 3
        assert results['failed_downloads'] == 0
        
        # Verify output files
        output_files = list(Path(temp_output_dir).glob('*.csv'))
        assert len(output_files) >= 3
        
        # Test that we can read the data back
        for csv_file in output_files:
            df = pd.read_csv(csv_file)
            # Just verify it loads without error
            assert isinstance(df, pd.DataFrame)
        
        await downloader.close_pool()

class TestConfigurationHandling:
    """Test configuration and environment variable handling"""
    
    @patch.dict(os.environ, {
        'DB_HOST': 'testhost',
        'DB_NAME': 'testdb',
        'DB_USER': 'testuser',
        'DB_PASSWORD': 'testpass',
        'MAX_CONNECTIONS': '15',
        'MAX_CONCURRENT_DOWNLOADS': '5'
    })
    def test_environment_variable_loading(self):
        """Test that environment variables are loaded correctly"""
        from dotenv import load_dotenv
        load_dotenv()
        
        assert os.getenv('DB_HOST') == 'testhost'
        assert os.getenv('MAX_CONNECTIONS') == '15'
        assert os.getenv('MAX_CONCURRENT_DOWNLOADS') == '5'