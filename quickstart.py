from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pandas as pd
import zeep
import datetime
import sys
import time
import io
import getpass

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1TUWtYoMy6EGgBEdPUb42O1xwTIQtlWpu-1fDxjhAyIk'
SAMPLE_RANGE_NAME = 'Employee!A:B'
BirstUsername = 'mjain@birst.com'
BirstPassword = getpass.getpass('Birst Password: ')
SpaceID = 'dad520c0-0d56-4811-98c0-5969b2af43e2'
CurrentDate = datetime.datetime.now()
wsdl = 'https://sde.birst.com/commandwebservice.asmx?wsdl'
client = zeep.Client(wsdl=wsdl)
LoginToken = client.service.Login(BirstUsername, BirstPassword)

"""Shows basic usage of the Sheets API.
Prints values from a sample spreadsheet.
"""
creds = None
# The file token.pickle stores the user's access and refresh tokens, and is
# created automatically when the authorization flow completes for the first
# time.
if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)
# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)

service = build('sheets', 'v4', credentials=creds)

# Call the Sheets API
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                            range=SAMPLE_RANGE_NAME).execute()
header = result.get('values', [])[0]  # Assumes first line is header!
values = result.get('values', [])[1:]  # Everything else is data.
global df

if not values:
    print('No data found.')
else:
    all_data = []
    for col_id, col_name in enumerate(header):
        column_data = []
        for row in values:
            column_data.append(row[col_id])
        ds = pd.Series(data=column_data, name=col_name)
        all_data.append(ds)
    df = pd.concat(all_data, axis=1)

print(df)

Export = df.to_csv(r'Export_DataFrame.csv', index=False)
time.sleep(5)
file_path = "Export_DataFrame.csv"

try:
    dataUploadToken = client.service.beginDataUpload(LoginToken, SpaceID, "Export_DataFrame.csv")

    data = open(file_path, 'rb').read()
    client.service.uploadData(LoginToken, dataUploadToken, len(data), data)
except Exception as e:
    print(e)
    print('Error occured while uploading file')
    sys.exit()

client.service.finishDataUpload(LoginToken, dataUploadToken)

while True:
    data_upload_status = client.service.isDataUploadComplete(LoginToken, dataUploadToken)
    if data_upload_status:
        break
    time.sleep(2)

if data_upload_status:
    print('upload successful')
else:
    print('upload failed')




client.service.Logout(LoginToken)
# go
# go
# go
# go
