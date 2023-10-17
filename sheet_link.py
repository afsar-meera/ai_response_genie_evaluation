from google.oauth2 import service_account
from googleapiclient.discovery import build
from config import BASE_PATH


def create_editable_sheet(df, file_name):
    credentials_path = f'{BASE_PATH}/credentials.json'

    # Authenticate with Google Drive using the JSON key file
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=['https://www.googleapis.com/auth/drive']
    )
    drive_service = build('drive', 'v3', credentials=credentials)

    # Create a new Google Sheet using the Google Sheets API
    sheets_service = build('sheets', 'v4', credentials=credentials)
    spreadsheet = sheets_service.spreadsheets().create().execute()
    sheet_id = spreadsheet['spreadsheetId']

    # Share the Google Sheet with edit access to @locobuzz.com users
    domain_permission = {
        'type': 'domain',
        'role': 'writer',
        'domain': 'locobuzz.com'
    }
    drive_service.permissions().create(
        fileId=sheet_id,
        body=domain_permission,
        sendNotificationEmail=False  # Optional: Set to True if you want to notify users
    ).execute()

    # Rename the Google Sheet (optional)
    new_sheet_name = file_name
    drive_service.files().update(fileId=sheet_id, body={'name': new_sheet_name}).execute()

    # Convert column names and data to a list of lists
    values = [df.columns.tolist()] + df.values.tolist()

    # Write the data to the Google Sheet
    range_name = 'Sheet1!A1'
    body = {'values': values}
    result = sheets_service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=range_name,
        valueInputOption='RAW',
        body=body
    ).execute()

    # Construct the URL for the Google Sheet
    sheet_url = f'https://docs.google.com/spreadsheets/d/{sheet_id}'

    return sheet_url
