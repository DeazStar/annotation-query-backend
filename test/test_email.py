import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import os
from pathlib import Path
from email import init_mail, convert_to_csv, send_email 

@pytest.fixture
def mock_app():
    return MagicMock()

@pytest.fixture
def mock_mail(mock_app):
    with patch('email_functionality.Mail') as mock_mail:
        yield mock_mail

@pytest.fixture
def sample_response():
    nodes = {
        'Person': [
            {'data': {'id': '1', 'name': 'Alice', 'age': 30}},
            {'data': {'id': '2', 'name': 'Bob', 'age': 25}}
        ],
        'Company': [
            {'data': {'id': '3', 'name': 'ACME Corp', 'founded': 1990}}
        ]
    }
    edges = {
        'WORKS_FOR': [
            {'data': {'source': 'Person 1', 'target': 'Company 3', 'since': 2015}},
            {'data': {'source': 'Person 2', 'target': 'Company 3', 'since': 2018}}
        ]
    }
    return (nodes, edges)

def test_init_mail(mock_app, mock_mail):
    init_mail(mock_app)
    mock_mail.assert_called_once_with(mock_app)

def test_convert_to_csv(sample_response, tmp_path):
    with patch('email_functionality.datetime') as mock_datetime:
        mock_datetime.datetime.now.return_value.strftime.return_value = '2023-05-20_10-00-00'
        with patch('email_functionality.Path') as mock_path:
            mock_path.return_value.resolve.return_value = tmp_path / '2023-05-20_10-00-00.xls'
            file_path = convert_to_csv(sample_response)
    
    assert file_path == tmp_path / '2023-05-20_10-00-00.xls'
    assert os.path.exists(file_path)

    # Verify the contents of the Excel file
    with pd.ExcelFile(file_path) as xls:
        assert set(xls.sheet_names) == {'Person', 'Company', 'Person-relationship-Company'}
        
        df_person = pd.read_excel(xls, 'Person')
        assert list(df_person.columns) == ['id', 'name', 'age']
        assert len(df_person) == 2

        df_company = pd.read_excel(xls, 'Company')
        assert list(df_company.columns) == ['id', 'name', 'founded']
        assert len(df_company) == 1

        df_relationship = pd.read_excel(xls, 'Person-relationship-Company')
        assert list(df_relationship.columns) == ['source', 'target', 'since']
        assert len(df_relationship) == 2

def test_send_email_success(mock_mail, sample_response, tmp_path):
    with patch('email_functionality.mail') as mock_mail_instance:
        with patch('email_functionality.convert_to_csv') as mock_convert_to_csv:
            mock_convert_to_csv.return_value = tmp_path / 'test.xls'
            
            send_email('Test Subject', ['test@example.com'], 'Test Body', sample_response)
            
            mock_mail_instance.send.assert_called_once()
            msg = mock_mail_instance.send.call_args[0][0]
            assert msg.subject == 'Test Subject'
            assert msg.recipients == ['test@example.com']
            assert msg.body == 'Test Body'
            assert len(msg.attachments) == 1
            assert msg.attachments[0].filename == 'test.xls'

def test_send_email_failure(mock_mail, sample_response, tmp_path, caplog):
    with patch('email_functionality.mail', None):
        with patch('email_functionality.convert_to_csv') as mock_convert_to_csv:
            mock_convert_to_csv.return_value = tmp_path / 'test.xls'
            
            send_email('Test Subject', ['test@example.com'], 'Test Body', sample_response)
            
            assert "Failed to send email: Can't send email" in caplog.text

def test_send_email_attachment_cleanup(mock_mail, sample_response, tmp_path):
    test_file = tmp_path / 'test.xls'
    test_file.touch()
    
    with patch('email_functionality.mail') as mock_mail_instance:
        with patch('email_functionality.convert_to_csv') as mock_convert_to_csv:
            mock_convert_to_csv.return_value = test_file
            
            send_email('Test Subject', ['test@example.com'], 'Test Body', sample_response)
            
            assert not os.path.exists(test_file)

if __name__ == '__main__':
    pytest.main(['-v', 'test_email_functionality.py'])