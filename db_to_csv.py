import sqlite3
import pandas as pd

# SQLite database file
DATABASE_FILE = "geocoding_data.db"
CSV_OUTPUT_FILE = "successful_geocodes.csv"


def export_successful_addresses_to_csv():
    """Export successful addresses with coordinates to a CSV file."""
    with sqlite3.connect(DATABASE_FILE) as conn:
        # Query to select the required data from the addresses table
        query = "SELECT address, latitude, longitude FROM addresses"

        # Using pandas to read the SQL query into a DataFrame
        df = pd.read_sql_query(query, conn)

        # Saving the DataFrame to a CSV file
        df.to_csv(CSV_OUTPUT_FILE, index=False)
        print(f"Data exported to {CSV_OUTPUT_FILE}")


if __name__ == "__main__":
    export_successful_addresses_to_csv()
