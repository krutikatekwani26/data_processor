from geopy.distance import geodesic
from .utils.helpers import read_yaml
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import hashlib
import pandas as pd

def check_city(dataset,schema_file):
    """
    Check and correct invalid city names in the dataset using the schema file.

    This function identifies city names in the dataset that are not present in the schema file.
    For each invalid city, it finds the nearest valid city in the same state and updates the dataset.

    :param schema_file: A dictionary representing the schema file, containing a list of valid cities.
    :param dataset: A pandas DataFrame containing the dataset to be checked.
    :return: None. The function modifies the dataset in place.
    """
    
    # Extract the column name for cities from the schema file
    col_name = list((schema_file['COLUMNS'].keys()))[1]

    # Get the set of valid cities from the schema file
    schema_city = set(schema_file['CITY'])

    # Convert the list of cities in the dataset column to a set
    df_city_toList = set(dataset[col_name])

    # Identify invalid cities by finding the difference between the dataset cities and the schema cities
    invalid_cities = df_city_toList - schema_city

    # If there are any invalid cities, proceed to find the nearest valid city
    if invalid_cities:
        for city in invalid_cities:
            # Find the nearest valid city for each invalid city
            nearest_city = find_nearest_valid_city(city)
            
            # Update the dataset with the nearest valid city
            dataset.loc[dataset[col_name] == city, col_name] = nearest_city


    return dataset

def find_nearest_valid_city(invalid_city):
    """
    Find the nearest valid city in the same state as the invalid city using pre-defined city coordinates.

    This function first retrieves the coordinates of the invalid city and then looks for the nearest
    valid city within the same state from a YAML file containing city coordinates.

    :param invalid_city: The name of the invalid city.
    :return: The name of the nearest valid city in the same state, or an error message if not found.
    """
    # Load the schema file containing city coordinates
    city_data = read_yaml('data_processor\input_files\city_coordinates.yaml')

    # Get coordinates for the invalid city
    invalid_city_data = get_city_coordinates(invalid_city)

    if not invalid_city_data:
        return "Invalid city coordinates not found."

    # Filter cities to those in the same state
    same_state_cities = {
        city: coords for city, coords in city_data.items()
        if coords['state'] == invalid_city_data['state']
    }

    # Prepare a list of cities and their distances to the invalid city
    distances = []
    invalid_city_coords = (invalid_city_data['latitude'], invalid_city_data['longitude'])

    for city, coords in same_state_cities.items():
        city_coords = (coords['latitude'], coords['longitude'])
        distance = geodesic(invalid_city_coords, city_coords).km
        distances.append((city, distance))

    # Sort the list by distance (ascending)
    distances.sort(key=lambda x: x[1])

    # Get the nearest city
    if distances:
        nearest_city, _ = distances[0]
        return nearest_city
    else:
        return "No valid city found in the same state."
    
def get_city_coordinates(city_name: str):
    """
    Get the geographical coordinates (latitude and longitude) and state for a given city name.

    This function uses the Nominatim geocoding service from the OpenStreetMap (OSM) project to
    fetch the geographical coordinates and state of a specified city within the USA.

    :param city_name: The name of the city to geocode.
    :return: A dictionary with the city's latitude, longitude, and state, or None if the city could not be geocoded.
    """
    geolocator = Nominatim(user_agent="city_geocoder")

    try:
        # Attempt to geocode the city name with an assumption it's in the USA
        location = geolocator.geocode(f"{city_name}, USA", addressdetails=True)

        if location and location.raw.get('address'):
            address = location.raw['address']

            # Extract state information if available
            state = address.get('state')

            return {
                'latitude': location.latitude,
                'longitude': location.longitude,
                'state': state
            }

    except GeocoderTimedOut:
        print(f"Geocoding timed out for city '{city_name}'.")

    except Exception as e:
        print(f"Error geocoding city '{city_name}': {e}")

    return None    

def generate_hash(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Generates an MD5 hash for each row in the DataFrame based on specific columns.
    
    :param dataframe: The DataFrame to process.
    :return: A DataFrame with an additional 'hash_id' column containing the MD5 hashes.
    """
    # Check if the required columns are present in the DataFrame
    required_columns = ['STATE', 'CITY', 'YEAR', 'LEVEL', 'TRADE']
    missing_columns = [col for col in required_columns if col not in dataframe.columns]
    
    if missing_columns:
        raise ValueError(f"The following required columns are missing from the DataFrame: {', '.join(missing_columns)}")

    # Define a helper function to create the tag string and hash it for each row
    def hash_row(row):
        tag_str = f"{row['STATE']}|{row['CITY']}|{row['YEAR']}|{row['LEVEL']}|{row['TRADE']}"
        return hashlib.md5(tag_str.encode()).hexdigest()

    # Apply the hash_row function to the DataFrame and create the 'hash_id' column
    dataframe['HASH ID'] = dataframe.apply(hash_row, axis=1)

    return dataframe

def keep_the_largest_dup(some_data: pd.DataFrame) -> pd.DataFrame:
    """
    Keep the largest duplicates based on 'BASE' and 'FRINGE' columns in descending order,
    and remove rows with duplicate 'hash_id'.

    This function sorts the DataFrame by 'BASE' and 'FRINGE' in descending order and
    then removes rows with duplicate 'hash_id', keeping the first occurrence.

    :param some_data: The DataFrame containing rows to be deduplicated.
    :return: The deduplicated DataFrame with the largest 'BASE' and 'FRINGE' values retained.
    """
    # Get the number of rows before deduplication
    before_deduplication = some_data.shape[0]
    
    # Sort the data by 'BASE' and 'FRINGE' in descending order
    some_data = some_data.sort_values(by=['BASE', 'FRINGE'], ascending=False, ignore_index=True)

    # Drop duplicates based on the 'hash_id' column, keeping the first occurrence
    some_data.drop_duplicates(subset=['HASH ID'], keep='first', inplace=True, ignore_index=True)
    
    # Get the number of rows after deduplication
    after_deduplication = some_data.shape[0]
    
    # Calculate and print the number of rows dropped
    rows_dropped = before_deduplication - after_deduplication
    print(f"Number of rows dropped during deduplication: {rows_dropped}")
    
    return some_data


def convert_journeyman_to_journey(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Converts all occurrences of '3-JOURNEYMAN' to '3-JOURNEY' in the 'LEVEL' column of the DataFrame.

    :param dataframe: The DataFrame containing the 'LEVEL' column.
    :return: The modified DataFrame with the updated 'LEVEL' column.
    """
    dataframe['LEVEL'] = dataframe['LEVEL'].replace('3-JOURNEYMAN', '3-JOURNEY')
    return dataframe

def handle_override( main_df: pd.DataFrame, override_df: pd.DataFrame):
        """
        Handle merging of override rows using the hash_id column.
    
        :param merged_df: Merged DataFrame to which overrides will be applied.
        :param override_df: DataFrame containing override rows.
        :param sheet_name: Name of the collection sheet being processed.
        :return: DataFrame with overrides applied.
        """
        override_df = filter_same_rows(main_df, override_df)

        # Get the set of hash_ids in the override DataFrame
        override_tags = set(override_df["HASH ID"])
        print(f"Number of override tags: {len(override_tags)}")

        # Identify rows in merged_df that need to be overridden
        to_override = main_df["HASH ID"].isin(override_tags)
        num_to_override = to_override.sum()
        print(f"Number of rows to override: {num_to_override}")
        
        # Remove the rows from merged_df that will be overridden
        main_df = main_df[~to_override]

        # Append the override rows to merged_df
        main_df = pd.concat([main_df, override_df], ignore_index=True)
        
        return main_df.drop(['COMBINED_HASH'], axis=1)

def replace_unconfirmed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace unconfirmed rows in the DataFrame with confirmed rows if available.

    This function searches for unconfirmed rows in the DataFrame that have a matching
    confirmed row (based on the 'hash_id' column). It then removes those unconfirmed rows
    from the DataFrame, ensuring that only the confirmed rows are retained.

    :param df: The DataFrame containing both confirmed and unconfirmed rows.
    :return: The DataFrame with unconfirmed rows replaced by confirmed rows.
    """
    # Separate confirmed and unconfirmed rows
    confirmed_rows = df[df['CONFIRMED'] == 1]
    unconfirmed_rows = df[df['CONFIRMED'] == 0]

    # Create a boolean mask for unconfirmed rows that have a matching confirmed row
    hash_mask = unconfirmed_rows['HASH ID'].isin(confirmed_rows['HASH ID'])

    # Identify unconfirmed rows to be dropped
    unconfirmed_rows_to_drop = unconfirmed_rows[hash_mask]
    print(f"Number of unconfirmed rows to drop: {len(unconfirmed_rows_to_drop)}")

    # Drop the identified unconfirmed rows from the DataFrame
    df = df.drop(unconfirmed_rows_to_drop.index)

    return df

def filter_same_rows(merged_df: pd.DataFrame, override_df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters out rows from the override DataFrame that are exactly the same as those in the merged DataFrame.
    This is determined by comparing a combined hash of the 'hash_id' and the integer parts of 'BASE' and 'FRINGE'.

    :param merged_df: The merged DataFrame.
    :param override_df: The override DataFrame containing rows to be merged into the merged DataFrame.
    :return: The filtered override DataFrame with rows that are not present in the merged DataFrame.
    """
    # Generate a combined hash for each row in merged_df by concatenating 'hash_id' and base_fringe_hash
    merged_df['COMBINED_HASH'] = merged_df['HASH ID'] + merged_df.apply(_generate_base_fringe_hash, axis=1)

    # Generate a combined hash for each row in override_df by concatenating 'hash_id' and base_fringe_hash
    override_df['COMBINED_HASH'] = override_df['HASH ID'] + override_df.apply(_generate_base_fringe_hash, axis=1)

    # Filter out rows in override_df that are exactly the same as those in merged_df
    filtered_override_df = override_df[~override_df['COMBINED_HASH'].isin(merged_df['COMBINED_HASH'])].copy()

    # Ensure we are working with a copy when dropping columns
    merged_df = merged_df.copy()
    filtered_override_df = filtered_override_df.copy()

    # Drop the COMBINED_HASH column from both DataFrames
    merged_df.drop(columns=['COMBINED_HASH'], inplace=True)
    filtered_override_df.drop(columns=['COMBINED_HASH'], inplace=True)
    override_df.drop(columns=['COMBINED_HASH'], inplace=True)

    return filtered_override_df

def _generate_base_fringe_hash(row: pd.Series) -> str:
    """
    Generates a hash for a given row based on the integer parts of 'BASE' and 'FRINGE' columns.
    This method ensures that rows with similar 'BASE' and 'FRINGE' values (when rounded to integers)
    generate the same hash, allowing for comparisons even if the exact values differ slightly.

    :param row: The row of the DataFrame.
    :return: A hash string representing the integer parts of 'BASE' and 'FRINGE'.
    """
    # Convert 'BASE' and 'FRINGE' to their integer parts
    base_int = int(row['BASE'])
    fringe_int = int(row['FRINGE'])

    # Create a string combining the integer parts of 'BASE' and 'FRINGE'
    row_str = f"{base_int}|{fringe_int}"

    # Generate and return the MD5 hash of the combined string
    return hashlib.md5(row_str.encode()).hexdigest()