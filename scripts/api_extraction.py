import requests
import pandas as pd
import os

def extract_api_data():
    import requests
    import pandas as pd
    # Define the flight IDs you want to retrieve
    flight_ids = [79952, 79953, 79956, 79957, 79958, 79959, 79960]

    # Your API token
    api_token = os.getenv("API_TOKEN")

    # Base URL for the API
    base_url = "https://test.fl3xx.com/api/external/flight/"

    # Headers for authentication
    headers = {
        "X-Auth-Token": api_token
    }
    flight_data_list = []
    # Loop through each flight ID and make a request
    for flight_id in flight_ids:
        response = requests.get(f"{base_url}{flight_id}", headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            flight_data_list.append(data)  # Print or process the data as needed
        else:
            print(f"Failed to retrieve data for flight ID {flight_id}: {response.status_code}")
    return pd.DataFrame(flight_data_list)




if __name__ == "__main__":
    flight_data = extract_api_data()
    print(f"Extracted data")

    project_path = os.getcwd()
    print(f"Project path: {project_path}")
    data_path = os.path.join(project_path, r'data\raw')
    print(f"Data path: {data_path}")
    os.makedirs(data_path, exist_ok=True)
    flight_data.to_csv(os.path.join(data_path, 'flight_data.csv'), index=False)
    print("Data saved to CSV file")