import csv
import os
from dotenv import load_dotenv
from databricks import sql

# Load environment variables from the .env file
load_dotenv()


def load(dataset="dataset/US_births_2000-2014_SSA.csv"):
    """
    Transforms and Loads data into the Databricks database.
    Args:
    dataset (str): Path to the CSV dataset (default: US Births dataset)
    """
    # Read the CSV file
    birth_data = csv.reader(open(dataset, newline=""), delimiter=",")
    next(birth_data)  # Skip the header row

    # Connect to Databricks using credentials from .env
    with sql.connect(
        server_hostname=os.getenv("HOST_NAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_API_KEY"),
    ) as connection:

        with connection.cursor() as cursor:
            # Create a table if it doesn't exist
            cursor.execute(
                """
            CREATE TABLE IF NOT EXISTS nmk43_births_data (
                year INT,
                month INT,
                date_of_month INT,
                day_of_week INT,
                births INT
            );
            """
            )

            # Check if data already exists
            cursor.execute("SELECT * FROM nmk43_births_data")
            result = cursor.fetchall()

            if not result:  # If no data, insert CSV data
                print("Inserting data into the nmk43_births_data table...")

                # Construct SQL insert statement
                string_sql = "INSERT INTO nmk43_births_data VALUES "

                for row in birth_data:  # Loop through the rows of birth data
                    string_sql += f"\n({int(row[0])}, {int(row[1])}, {int(row[2])}, {int(row[3])}, {int(row[4])}),"

                # Remove trailing comma and add semicolon
                string_sql = string_sql[:-1] + ";"

                # Execute the insert statement
                cursor.execute(string_sql)
                print("Data inserted successfully!")
            else:
                print("Data already exists in the nmk43_births_data table.")

        # Close the connection
        connection.close()


# Example usage
if __name__ == "__main__":
    load()
