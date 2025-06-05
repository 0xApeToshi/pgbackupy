import pytest
import asyncio
import sys
from pathlib import Path

# Configure pytest for async tests
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()

# Add the project root to Python path so we can import pgbackupy
@pytest.fixture(scope="session", autouse=True)
def setup_path():
    """Add project root to Python path"""
    project_root = Path(__file__).parent.parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

# Configure asyncio mode for pytest-asyncio
pytest_plugins = ('pytest_asyncio',)