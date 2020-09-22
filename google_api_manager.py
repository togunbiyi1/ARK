import pickle, warnings, uuid
import os.path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import pandas as pd
from logging_config import get_logger

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

google_api_manager_logger = get_logger("ark.google_api_manager")

def gsheet_api_check(SCOPES):
    google_api_manager_logger.info("running gsheet_api_check")
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds


def pull_sheet_data(SCOPES, SPREADSHEET_ID, RANGE_NAME):
    google_api_manager_logger.info("running pull_sheet_data")
    creds = gsheet_api_check(SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result = sheet.values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=RANGE_NAME).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        rows = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                  range=RANGE_NAME).execute()
        data = rows.get('values')
        print("COMPLETE: Data copied")
        return data


def run_gsheets_manager():
    google_api_manager_logger.info("running run_gsheets_manager")
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SPREADSHEET_ID = '1MwhS_L7JEDlr0a7wwIzI7rtDHjoKF0Uf84hr0htN5C8'
    RANGE_NAME = 'Form responses 1'
    data = pull_sheet_data(SCOPES, SPREADSHEET_ID, RANGE_NAME)

    gsheet_responses = pd.DataFrame(data[1:], columns=data[0])
    gsheet_responses = gsheet_responses.iloc[1:].reset_index(drop=True)
    new_timestamps = list(gsheet_responses["Timestamp"])
    print(new_timestamps)

    old_responses = pd.read_csv('responses/ark_responses_5.csv')
    old_timestamps = list(old_responses["Timestamp"])
    print(old_timestamps)

    new_timestamps = [timestamp for timestamp in new_timestamps if timestamp not in old_timestamps]
    print(new_timestamps)
    if len(new_timestamps) == 0:
        print("no new new_timestamps")
        return False
    else:
        print("new_timestamps")
        new_rows = gsheet_responses.loc[gsheet_responses["Timestamp"].isin(new_timestamps)]
        new_rows['Bio: id'] = [uuid.uuid4() for _ in range(len(new_rows.index))]
        print(new_rows)
        new_responses = pd.concat([old_responses, new_rows])
        print(new_responses)
        new_responses.to_csv('responses/ark_responses_5.csv', index=False)
        return new_rows


if __name__ == '__main__':
    run_gsheets_manager()
    # For adding UUID column to existing csv
    # df = pd.read_csv('responses/ark_responses_4.csv')
    # df['Bio: id'] = [uuid.uuid4() for _ in range(len(df.index))]
    # df.to_csv('responses/ark_responses_4.csv', index=False)
