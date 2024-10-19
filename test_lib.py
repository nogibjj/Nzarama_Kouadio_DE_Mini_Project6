from mylib.extract import extract
from mylib.transform_load import load
from mylib.query import query
import os


# Test extract function
def test_extract():
    file_path = "dataset/US_births_2000-2014_SSA.csv"
    extract()
    # Test if the file was downloaded successfully
    assert os.path.exists(file_path), "CSV file was not downloaded."
    assert file_path.endswith(".csv"), "Downloaded file is not a CSV."
    print("Extract function passed.")


# Test transform and load function
def test_transform_load():
    file_path = "dataset/US_births_2000-2014_SSA.csv"
    # Check if file exists before loading (to ensure extract ran correctly)
    assert os.path.exists(file_path), "CSV file is missing for transform and load."
    # Load the data into Databricks and check if it was successful
    try:
        load(file_path)
        print("Transform and load function passed.")
    except Exception as e:
        assert False, f"Transform and load failed with error: {e}"


# Test query function
def test_query():
    try:
        query()
        print("Query function passed.")
    except Exception as e:
        assert False, f"Query function failed with error: {e}"


# Run tests
if __name__ == "__main__":
    test_extract()
    test_transform_load()
    test_query()
