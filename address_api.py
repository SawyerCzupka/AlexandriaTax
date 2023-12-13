import pandas as pd
import requests
import time
import json
import logging
import sqlite3
from requests.exceptions import RequestException
from tqdm.auto import tqdm

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# SQLite database file
DATABASE_FILE = "geocoding_data.db"


def create_database():
    """Create the database and tables."""
    with sqlite3.connect(DATABASE_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS addresses (
                address TEXT PRIMARY KEY,
                latitude REAL,
                longitude REAL
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS failed_addresses (
                address TEXT PRIMARY KEY
            )
        """
        )
        conn.commit()


def save_coordinates(address, lat, lon):
    """Save coordinates to the database."""
    with sqlite3.connect(DATABASE_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO addresses (address, latitude, longitude) VALUES (?, ?, ?)",
            (address, lat, lon),
        )
        conn.commit()


def save_failed_address(address):
    """Save failed address to the database."""
    with sqlite3.connect(DATABASE_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO failed_addresses (address) VALUES (?)", (address,)
        )
        conn.commit()


def geocode(address):
    """Geocode an address using the Nominatim API."""
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": address, "format": "json"}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
    except RequestException as e:
        tqdm.write(f"Request failed for address '{address}': {e}")
        return None

    try:
        results = response.json()
        if not results:
            tqdm.write(f"No results found for address '{address}'")
            return None
    except json.JSONDecodeError as e:
        tqdm.write(f"Error decoding JSON response for address '{address}': {e}")
        return None

    lat = results[0]["lat"]
    lon = results[0]["lon"]
    return lat, lon


def geocode_addresses(addresses, delay=0):
    """Geocode a list of addresses with a delay to avoid rate limiting."""
    with tqdm(addresses, desc="Geocoding addresses", unit="address") as pbar:
        for address in pbar:
            coords = geocode(address)
            if coords:
                lat, lon = coords
                save_coordinates(address, lat, lon)
                tqdm.write(f"Found coords for '{address}': {coords}")
            else:
                save_failed_address(address)
                # tqdm.write(f"Failed to geocode address '{address}'")
            time.sleep(delay)  # Delay to respect rate limiting


if __name__ == "__main__":
    create_database()  # Initialize the database
    try:
        data = pd.read_csv("data/Alexandria_Tax_Info.csv")
        addresses = data["A_Properties"].unique()
        logging.info(f"Loaded {len(addresses)} addresses from file.")
    except FileNotFoundError as e:
        logging.error(f"Error reading CSV file: {e}")
    except pd.errors.EmptyDataError as e:
        logging.error(f"CSV file is empty: {e}")
    except Exception as e:
        logging.error(f"An unexpected error occurred while reading the CSV file: {e}")
    else:
        geocode_addresses(addresses)
