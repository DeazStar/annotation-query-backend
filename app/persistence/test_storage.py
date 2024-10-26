import pytest
from unittest.mock import patch, MagicMock
from app.models.storage import StorageService, Storage
from pymongoose.mongo_types import MongoException

@pytest.fixture
def storage_service():
    return StorageService()

@pytest.fixture
def mock_storage():
    return MagicMock(spec=Storage)

def test_save_new_query(storage_service, mock_storage):
    # Arrange
    user_id = "user123"
    query = "test query"
    title = "Test Title"
    summary = "This is a test summary."
    
    # Mock the find method to return None, simulating that no query exists
    with patch.object(Storage, 'find', return_value=None):
        # Mock the save method
        with patch.object(mock_storage, 'save', return_value=None):
            # Act
            result = storage_service.save(user_id, query, title, summary)

            # Assert
            assert result == {"message": "Query saved successfully"}

def test_save_existing_query(storage_service, mock_storage):
    # Arrange
    user_id = "user123"
    query = "test query"
    title = "Test Title"
    summary = "This is a test summary."

    # Mock the find method to return an existing query
    with patch.object(Storage, 'find', return_value=mock_storage):
        # Act
        result = storage_service.save(user_id, query, title, summary)

        # Assert
        assert isinstance(result, dict)
        assert "error" in result
        assert "Query already exists for user_id" in result["error"]

def test_get_user_data(storage_service, mock_storage):
    # Arrange
    user_id = "user123"
    mock_storage.return_value = {"query": "test query"}

    with patch.object(Storage, 'find', return_value=mock_storage):
        # Act
        result = storage_service.get(user_id)

        # Assert
        assert result == mock_storage.return_value

def test_get_user_data_not_found(storage_service):
    # Arrange
    user_id = "user123"

    with patch.object(Storage, 'find', return_value=None):
        # Act
        result = storage_service.get(user_id)

        # Assert
        assert isinstance(result, dict)
        assert "error" in result
        assert "No data found for user_id" in result["error"]

def test_get_all_user_data(storage_service):
    # Arrange
    user_id = "user123"
    mock_data = [{"query": "test query 1"}, {"query": "test query 2"}]

    with patch.object(Storage, 'find', return_value=mock_data):
        # Act
        result = storage_service.get_all(user_id, page_number=1)

        # Assert
        assert result == mock_data

def test_get_by_id(storage_service, mock_storage):
    # Arrange
    id = "605c72f9b94f5c001f8d3f08"

    with patch.object(Storage, 'find_by_id', return_value=mock_storage):
        # Act
        result = storage_service.get_by_id(id)

        # Assert
        assert result == mock_storage

def test_get_by_id_not_found(storage_service):
    # Arrange
    id = "605c72f9b94f5c001f8d3f08"

    with patch.object(Storage, 'find_by_id', return_value=None):
        # Act
        result = storage_service.get_by_id(id)

        # Assert
        assert isinstance(result, dict)
        assert "error" in result
        assert "No data found with id" in result["error"]
