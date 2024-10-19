from dotenv import load_dotenv
from databricks import sql
import os

# Define a query that joins the same table (fake join)
# Query also contains aggregation, and sorting
complex_query = """
WITH summary_births AS (
    SELECT year, month, SUM(births) as total_births
    FROM nmk43_births_data
    GROUP BY year, month
)
SELECT a.year, a.month, a.total_births, b.day_of_week, b.births
FROM summary_births a
JOIN nmk43_births_data b
ON a.year = b.year AND a.month = b.month
ORDER BY a.year, a.month, b.day_of_week;
"""


def query():
    """
    Query the nmk43_births_data table with a fake join and retrieve the results.
    """
    load_dotenv()

    # Connect to Databricks using credentials from .env
    with sql.connect(
        server_hostname=os.getenv("HOST_NAME"),
        http_path=os.getenv("HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_API_KEY"),
    ) as connection:

        with connection.cursor() as cursor:
            # Execute the SQL query
            cursor.execute(complex_query)
            result = cursor.fetchall()

            # Print each row in the result set
            for row in result:
                print(row)

            # Close the cursor and connection
            cursor.close()
        connection.close()


# Example usage
if __name__ == "__main__":
    query()
