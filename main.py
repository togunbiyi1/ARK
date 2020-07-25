import pandas as pd
from datetime import datetime
from data_manager import DataManager
from user_application import UserApp
import warnings
import os
print(os.getcwd())
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)

if __name__ == '__main__':

    run_through = DataManager(responses_filename='responses/ark_responses_2.csv')
    score_dict = run_through.run_data_manager()

    users = list(score_dict["user_info"].index)

    for user in users:
        app = UserApp(name=user, score_dict=score_dict)
        matches_df = app.flat_viability()
        print(matches_df)
        print("\n")
        matches_df.to_csv('results/{}_{}.csv'.format(user, datetime.today().strftime("%m_%d_%Y")))
        print("user: {} added".format(user))

