import pytest
from utils.helper import *
    
@pytest.fixture
def dummy_data():
    """Fixture to provide a sample DataFrame for testing"""
    data = {
        'name': [' Alice ', 'Bob ', 'Carla ', ' alice'],
        'amount': ['$500', '300', '$700', '300'],
        'punctuated_text': ['Hello , World !', 'Good :bye -', 'Test , case -', ' example ;'],
        'hash_id': [1, 2, 1, 4]
    }
    return pd.DataFrame(data)

@pytest.fixture
def schema():
    """Fixture for a dummy schema"""
    return {
        'COLUMNS': {
            'name': None,
            'amount': None,
            'punctuated_text': None,
            'hash_id': None
        },
        'VALID_VALUES': {
            'amount': ['300', '$500', '$700']
        }
    }


def test_make_uppercase(dummy_data):
    processor = make_uppercase
    result = processor(dummy_data)
    expected = ['NAME', 'AMOUNT', 'PUNCTUATED_TEXT', 'HASH_ID']
    assert all(result.columns == expected)
    assert all(result['NAME'] == [' ALICE ', 'BOB ', 'CARLA ', ' ALICE'])

def test_remove_spaces_around_punctuation(dummy_data):
    processor = remove_spaces_Around_punctuation
    result = processor(dummy_data)
    assert result['punctuated_text'][0] == 'Hello,World!'
    assert result['punctuated_text'][1] == 'Good:bye-'
    assert result['punctuated_text'][2] == 'Test,case-'
    assert result['punctuated_text'][3] == ' example;'


def test_manage_special_characters(dummy_data):
    processor = manage_special_characters
    result = processor(dummy_data)
    assert result['punctuated_text'][0] == 'Hello-World-'
    assert result['punctuated_text'][1] == 'Good-bye-'
    assert result['punctuated_text'][2] == 'Test-case-'
    assert result['punctuated_text'][3] == ' example-'


def test_strip_leading_and_trailing_spaces(dummy_data):
    processor = strip_leading_and_trailing_spaces
    result = processor(dummy_data)
    assert result['name'][0] == 'Alice'
    assert result['name'][1] == 'Bob'
    assert result['name'][2] == 'Carla'
    assert result['name'][3] == 'alice'


def test_clean_numeric_values(dummy_data):
    processor = clean_numeric_values
    result = processor(dummy_data)
    assert result['amount'][0] == 500.0
    assert result['amount'][1] == 300.0
    assert result['amount'][2] == 700.0
    assert result['amount'][3] == 300.0


def test_remove_duplicates(dummy_data):
    processor = remove_duplicates
    result = processor(dummy_data, subset=['hash_id'], keep='first')
    assert len(result) == 3
    assert result['hash_id'].tolist() == [1, 2, 4]


def test_drop_invalid_columns(dummy_data, schema):
    processor = drop_invalid_columns
    result = processor(dummy_data, schema)
    assert 'punctuated_text' in result.columns  
    assert 'hash_id' in result.columns          


def test_validate_column_values(dummy_data, schema):
    processor = validate_column_values
    # All valid values
    result = processor(dummy_data, schema)
    assert 'amount' in result.columns

    # Modify 
    dummy_data_invalid = dummy_data.copy()
    dummy_data_invalid.at[0, 'amount'] = 'INVALID'
    
    # This should raise a ValueError
    with pytest.raises(ValueError, match="Invalid values in column 'amount'"):
        processor(dummy_data_invalid, schema)


def test_read_yaml(tmp_path):
    
    yaml_content = """
    COLUMNS:
      name: null
      amount: null
    """
    yaml_file = tmp_path / "schema.yaml"
    yaml_file.write_text(yaml_content)
    
    result = read_yaml(yaml_file)
    assert result['COLUMNS']['name'] is None
    assert result['COLUMNS']['amount'] is None