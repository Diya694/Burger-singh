import jwt
import datetime
import requests
import urllib.request, urllib.parse, urllib.error
import json
import ssl
import pandas as pd
import numpy as np
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE
from gspread_dataframe import set_with_dataframe
import gspread
import time
from oauth2client.service_account import ServiceAccountCredentials

## function to fetch data using api

import datetime
def fetch_data(branchlist):
    # Define sales data and secret
    sales_data = {
        "iss": "220d6187-0a37-48db-aad2-673bea63844c",
        "iat": datetime.datetime.utcnow()
    }
    my_secret = 'cMbmAWWw1-naGH1Xhf80qU4AGtqqM1VrmGMb2p1_Pt4'
    token = jwt.encode(
        payload=sales_data,
        key=my_secret,
        algorithm="HS256"
    )

    # Define empty DataFrame to store combined data
    combined_df = pd.DataFrame()

    # Iterate over the last 7 days
    for i in range(1,8):
        # Calculate the date for the current iteration
        date_to_fetch = datetime.datetime.utcnow() - datetime.timedelta(days=i)

        # Iterate over branches and fetch data for the current date
        for branchname in branchlist:
            try:
                # Initialize lastKey to None for the first call
                last_key = None

                while True:
                    # Define API URL and parameters
                    url = "https://api.ristaapps.com/v1/inventory/indents/page"
                    paras = {
                        "branch": branchname,
                        "day": date_to_fetch.strftime("%Y-%m-%d"),
                        "lastKey": last_key
                    }

                    # Define headers
                    headers = {
                        'x-api-key': "220d6187-0a37-48db-aad2-673bea63844c",
                        'x-api-token': token,
                        'content-type': 'application/json'
                    }

                    # Make API request
                    response = requests.get(url, params=paras, headers=headers)
                    data = response.json()

                    # Extract records from response
                    records = data.get('data', [])

                    # Append records to combined_df
                    combined_df = pd.concat([combined_df, pd.DataFrame(records)], ignore_index=True)

                    # Check if there are more records to fetch
                    if 'lastKey' in data:
                        last_key = data['lastKey']
                    else:
                        # No more records to fetch, break out of the loop
                        break

                # Print success message for the branch and date
                # print("Data fetched successfully for branch:", branchname, "on", date_to_fetch.strftime("%Y-%m-%d"))

            except Exception as e:
                # Print error message and branch code if an error occurs
                print("Error occurred for branch:", branchname, "on", date_to_fetch.strftime("%Y-%m-%d"))
                print(e)

    return combined_df

# branchlist =["DSS"]

branchlist=["SC", "HO", "SLK", "KM", "NDYLD", "PSK", "PMF", "PV", "NIT", "DK", "DMD", 
"VMD", "MRD", "KK", "JNK", "CSD", "KN", "PSD", "GTB", "DDC", "VK", "S18", "PND", 
"ROH", "CCB", "MRC", "CP", "CCM", "MSA", "GCJ", "VBB", "S49", "S13K", "GHS", "DDCK",
"GGRH", "SMP", "PMMM", "BRP", "CLA", "VNJ", "N137", "HNS", "IP", "STA", "AL", "RAA",
"CRMS", "GN", "CD", "CHN", "GC1", "M2B2", "SNB", "ASH", "RCM", "VCR", "CED", "RJN", 
"BRH", "OBPTHB", "S7GGN", "TFSNM", "RCR", "MB", "SJ", "NTPA", "RCG", "KP", "NDAM", 
"VNB", "MTJ", "SKDM", "SNL", "SND", "FHG", "RCG1D", "MNJ", "GSPS", "CCMA", "DDT", 
"DCMB", "SBA", "RMD", "PVD", "WK", "NCR WH", "SP", "AMP", "MMS", "JWM", "MND", 
"NFC", "S122", "HKYLD", "SDM", "RNG", "LN", "VNI", "NNB", "JK", "CPK", "GFP", 
"IN", "LMB", "AL1", "TDI", "OMS", "SNP", "CLJ", "RPJ", "RRR", "AKR", "MVD", "ND",
"CLR", "S35CHD", "JAK", "S79", "AMG", "CCG", "PMG", "VVD", "KGD", "AMSD", "CSP",
"SNM", "TR", "EMS8G", "VRZ", "KKB", "IRLD", "CLP", "KB", "DDD", "LNH", "TNG", "SDR",
"ALM", "BRI", "ANU", "JMD", "CCD", "MVPD", "NDH", "DMRL", "N2CN", "VCS", "PMMJ",
"WA", "CDH", "CN", "WDGGN", "SCR", "BGN", "JCR", "WTJ", "KBD", "SMRJ", "ENPD", "DSS", "NHP", "NS52D", "PNDMRC", 
"MMS28G", "ASBP", "WPCP", "BBD", "TMSB", "HRMG", "WDD", "VCM", "USMU", "PMPD"]
# print(len(branchlist))
# Divide the branchlist into four parts

num_batches = 10

# Calculate the batch size and the remainder
batch_size, remainder = divmod(len(branchlist), num_batches)

# Create the batches
branch_batches = []
start = 0
for i in range(num_batches):
    extra = 1 if i < remainder else 0
    end = start + batch_size + extra
    branch_batches.append(branchlist[start:end])
    start = end


# num_batches = 20
# batch_size = len(branchlist) // num_batches

# branch_batches = [branchlist[i:i+batch_size] for i in range(0, len(branchlist), batch_size)]

# Initialize empty DataFrame to store combined data
combined_dfs = pd.DataFrame()

# Loop through each batch of branches
for branch_batch in branch_batches:
  print(branch_batch)
  # Fetch data for the current batch
  batch_data = fetch_data(branch_batch)
  # Concatenate the fetched data with the existing combined data
  combined_dfs = pd.concat([combined_dfs, batch_data], ignore_index=True)
  print(combined_dfs.shape)

combined_dfs=combined_dfs.rename(columns={'branchCode':'WarehouseCode','branchName':'WarehouseName'})

l_bname=[]
l_bcode=[]

for i in range(combined_dfs.shape[0]):
  l_bcode.append(combined_dfs['fromBranch'][i]['branchCode'])
  l_bname.append(combined_dfs['fromBranch'][i]['branchName'])
combined_dfs['StoreCode']=l_bcode
combined_dfs['StoreName']=l_bname

combined_dfs.drop(columns=['fromBranch'], inplace=True)

import pandas as pd

# Load the CSV file
# file_path = 'path_to_your_csv.csv'  # Update this to the path of your CSV file
# combined_dfs = pd.read_csv(file_path)

# Ensure there's an index column that we can use for merging later
combined_dfs.reset_index(inplace=True)

def normalize_items(row):
    items = row['items']
    if isinstance(items, list):
        # Normalize the list of dictionaries to a DataFrame
        items_df = pd.json_normalize(items)
        # Add the original index to track back to the parent row
        items_df['parent_index'] = row['index']
        return items_df
    else:
        return pd.DataFrame()

# Apply the function to each row in the DataFrame to create a new DataFrame for items
item_rows = combined_dfs.apply(normalize_items, axis=1)
flattened_items = pd.concat(item_rows.values.tolist(), ignore_index=True)

# Merge the flattened items DataFrame with the original DataFrame
# Choose the columns you want to keep from the original DataFrame
columns_to_keep = ['index', 'WarehouseCode', 'WarehouseName', 'StoreCode', 'StoreName', 'indentNumber', 'indentDate',
                   'indentBusinessDay', 'itemCount', 'itemsAmount', 'taxAmount', 'totalAmount', 'status', 'fulfillmentStatus','expectedDeliveryDate']  # Update as needed
# Perform a right join to keep all entries from flattened_items and matching entries from cleaned_data
merged_data = pd.merge(combined_dfs[columns_to_keep], flattened_items, left_on='index', right_on='parent_index', how='right')

# Drop the extra 'index' columns if they are no longer needed
merged_data.drop(columns=['index', 'parent_index'], inplace=True, errors='ignore')


# Display the first few rows of the merged DataFrame
# print(merged_data.head())

# Save the combined DataFrame to a Google Sheets spreadsheet
# Connect to Google Sheets
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("bs-supply-chain-a81a29a41d98.json", scope)
client = gspread.authorize(creds)

# # Open the spreadsheet (replace 'your_spreadsheet_key' with your actual spreadsheet key)
spreadsheet_key = '1WknRDDFzH7ED_pD2juInAUbqW72GdXIiPi3rOczISsU'  
spreadsheet = client.open_by_key(spreadsheet_key)


# Select the first worksheet
# worksheet = spreadsheet.get_worksheet(1)
worksheet = spreadsheet.worksheet("RawData")
worksheet.clear()
set_with_dataframe(worksheet,merged_data,include_index=False)


