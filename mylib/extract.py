import requests
import pandas as pd


def extract():
    url = "https://github.com/fivethirtyeight/data/raw/refs/heads/master/births/US_births_2000-2014_SSA.csv"
    file_path = "dataset/US_births_2000-2014_SSA.csv"

    # Download the file
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)

    # Print confirmation message
    print(f"File downloaded successfully and saved to {file_path}")

    # Load and return the DataFrame
    data = pd.read_csv(file_path)
    return data


# Example usage
if __name__ == "__main__":
    data = extract()
    print(data.head())  # Display the first few rows
