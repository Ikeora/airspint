import os
import logging
import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import azure.functions as func

# Load environment variables
load_dotenv()

# Azure configurations
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
RAW_CONTAINER_NAME = "rawdata"
CLEANED_CONTAINER_NAME = "cleandata"

TABLES = ["Aircraft", "Opportunity", "flight_data", "Contact", "invoices", "Asset", "Account", "Ownership"]

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="etl")
def etl(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        process_files()
        return func.HttpResponse("Files processed successfully.", status_code=200)
    except Exception as e:
        logging.error(f"Error processing files: {str(e)}")
        return func.HttpResponse(f"Error processing files: {str(e)}", status_code=500)

def download_from_blob():
    """Download all files from Azure Blob Storage to a local directory."""
    logging.info("Connecting to Azure Blob Storage...")
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(RAW_CONTAINER_NAME)

    local_dir = "/tmp"
    os.makedirs(local_dir, exist_ok=True)

    file_paths = []
    for blob in container_client.list_blobs():
        if blob.name in [f"{table}.csv" for table in TABLES]:  # Only download required tables
            file_path = os.path.join(local_dir, blob.name)
            blob_client = container_client.get_blob_client(blob.name)
            with open(file_path, "wb") as file:
                file.write(blob_client.download_blob().readall())
            logging.info(f"Downloaded: {blob.name}")
            file_paths.append(file_path)
    
    return file_paths

def clean_aircraft(df):
    """Clean Aircraft table."""
    df.columns = [col.strip().lower().replace("_", " ")[:-1].rstrip() for col in df.columns]
    df["registration"] = df["registration"].str.replace("C-", "", regex=False)
    df.fillna("Unknown", inplace=True)
    df.drop_duplicates(inplace=True)
    return df

def clean_invoices(df):
    """Clean Invoices table."""
    # Convert date columns to datetime (if applicable)
    df['INVDATE'] = pd.to_datetime(df['INVDATE'],format='%Y%m%d')
                                
    df.columns = [col.strip().lower() for col in df.columns]

    df.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Drop duplicates
    df.drop_duplicates(inplace=True)

    return df


def clean_opportunity(df):
    """Clean Opportunity table."""
    df["CreatedDate"] = pd.to_datetime(df["CreatedDate"], errors="coerce").dt.date
    df["IsWon"] = df["IsWon"].apply(lambda x: bool(x))
    df.drop_duplicates(inplace=True)
    df.columns = [col.strip().lower().replace("__c", "") for col in df.columns]
    return df

def clean_asset(df):
    """Clean Asset table."""
    # Standardize column names
    df.columns = [col.strip().lower().replace('__c', '') for col in df.columns]

    df=df[df['opportunity'].notnull()]

    # Fill missing values
    df.fillna("Unknown", inplace=True)

    # Drop duplicates
    df.drop_duplicates(inplace=True)

    return df

def clean_flight_data(df):
    """Clean Flight_Data table."""
    df = df[['flightId', 'quoteId', 'accountId', 'flightNumber', 
             'registrationNumber', 'airportFrom', 'airportTo', 'eta', 'etd']]
    df["registrationNumber"] = df["registrationNumber"].str.replace("C-", "", regex=False)
    
    airport_mapping = {
        'CYYC': 'Calgary', 'CYYZ': 'Toronto', 'KSLC': 'Salt Lake City',
        'KLAS': 'Las Vegas', 'CYVR': 'Vancouver', 'KMSO': 'Missoula',
        'KMIO': 'Miami', 'CYXE': 'Saskatoon', 'CYLW': 'Kelowna',
        'KREG': 'Regina', 'CYED': 'Edmonton', 'US-0222': 'Unknown'
    }
    
    df['airportFrom'] = df['airportFrom'].map(airport_mapping).fillna('Unknown')
    df['airportTo'] = df['airportTo'].map(airport_mapping).fillna('Unknown')
    df['eta'] = pd.to_datetime(df['eta'])
    df['etd'] = pd.to_datetime(df['etd'])

    df.columns = [col.lower() for col in df.columns]

    df.drop_duplicates(inplace=True)
    
    return df

def clean_account(df):
    """Split Account table into Account and Ownership tables."""
    aircrafts_data = []
    
    # Iterate through the original DataFrame and extract aircraft information
    for _, row in df.iterrows():
        account_id = row['Id']
        
        # Aircraft 1
        if pd.notna(row['Aircraft_Type_Owned__c']):
            aircrafts_data.append({
                'Account_Id': account_id,
                'Aircraft_Type_Owned__c': row['Aircraft_Type_Owned__c'],
                'Aircraft_Ownership__c': row['Aircraft_Ownership__c'],
                'Lease_Renewal_Date__c': row['Lease_Renewal_Date__c']
            })
        
        # Aircraft 2
        if pd.notna(row['Aircraft_Type_Owned_2_c']):
            aircrafts_data.append({
                'Account_Id': account_id,
                'Aircraft_Type_Owned__c': row['Aircraft_Type_Owned_2_c'],
                'Aircraft_Ownership__c': row['Aircraft_Ownership_2__c'],
                'Lease_Renewal_Date__c': row['Lease_Renewal_Date_2__c']
            })
        
        # Aircraft 3
        if pd.notna(row['Aircraft_Type_Owned_3__c']):
            aircrafts_data.append({
                'Account_Id': account_id,
                'Aircraft_Type_Owned__c': row['Aircraft_Type_Owned_3__c'],
                'Aircraft_Ownership__c': row['Aircraft_Ownership_3__c'],
                'Lease_Renewal_Date__c': row['Lease_Renewal_Date_3__c']
            })

    
    df = df[['Id', 'Fl3xx_Id__c', 'Name', 'Primary_Contact__c']]
    df.columns = [col.strip().lower().replace("__c", "") for col in df.columns]
    
    ownership_df = pd.DataFrame(aircrafts_data)

    ownership_df['Aircraft_Ownership_hours']=(
        ownership_df["Aircraft_Ownership__c"].str.extract(r'(\d+)').astype(int)
                                              )
    ownership_df.drop(columns=['Aircraft_Ownership__c'],inplace=True)

    ownership_df["Lease_Renewal_Date__c"] = pd.to_datetime(ownership_df["Lease_Renewal_Date__c"], errors="coerce")

    ownership_df.columns = ([col.strip().lower().replace('__c',"") 
                                for col in ownership_df.columns]
                                )

    return df, ownership_df


def upload_to_blob(df, table_name):
    """Upload cleaned data to Azure Blob Storage."""
    try:
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(CLEANED_CONTAINER_NAME)

        # Convert DataFrame to CSV
        csv_data = df.to_csv(index=False)
        
        # Upload to Blob Storage
        blob_client = container_client.get_blob_client(f"{table_name}.csv")
        blob_client.upload_blob(csv_data, overwrite=True)
        logging.info(f"Data successfully uploaded to {CLEANED_CONTAINER_NAME}/{table_name}.csv.")

    except Exception as e:
        logging.error(f"Blob Upload Error for {table_name}: {str(e)}")

def process_files():
    """Process all CSV files from Blob Storage."""
    files = download_from_blob()
    
    for file in files:
        table_name = os.path.basename(file).replace(".csv", "")
        df = pd.read_csv(file)
        
        # Apply cleaning functions
        if table_name == "Aircraft":
            df = clean_aircraft(df)
        elif table_name == "Opportunity":
            df = clean_opportunity(df)
        elif table_name == "flight_data":
            df = clean_flight_data(df)
        elif table_name =="invoices":
            df= clean_invoices(df)
        elif table_name =="Asset":
            df= clean_asset(df)
        elif table_name == "Account":
            df, ownership_df = clean_account(df)
            upload_to_blob(ownership_df, "Ownership")  # Extra table

        # Upload cleaned data to Blob Storage
        upload_to_blob(df, table_name)

    logging.info("All files processed and uploaded to Blob Storage.")
