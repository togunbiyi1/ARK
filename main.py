import os
import warnings
from datetime import datetime

import pandas as pd

from ARK.data_manager import DataManager
from ARK.email_manager import EmailManager
from ARK.scoring import BulkScoring, SingleScoring
from ARK.user_application import UserApp
from logging_config import get_logger

directory_list = ['data_tables', 'log', 'responses', 'results']
for directory in directory_list:
    if not os.path.exists(directory):
        os.makedirs(directory)

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)



def main(mode, user_name=None):
    main_logger = get_logger("ark.main")
    main_logger.info("*" * 5 + " starting main ark application " + "*" * 5)

    try:
        main_logger.info("initiate DataManager object")
        data_manager = DataManager(responses_filename='responses/ark_responses_4.csv')
        main_logger.info("running run_data_manager")
        data_manager.save_tables()
    except Exception as e:
        main_logger.error("Error running DataManager: {}".format(e))
        raise e

    if mode == 'bulk':
        try:
            main_logger.info("initiate BulkScoring object")
            scoring_manager = BulkScoring(data_dir='data_tables')
            main_logger.info("running scoring_manager")
            score_dict = scoring_manager.run_bulk_scoring()

            users = list(score_dict["user_info"].index)
        except Exception as e:
            main_logger.error("Error running BulkScoring: {}".format(e))
            raise e

        for user in users:
            try:
                main_logger.info("initiate UserApp object for user: {}".format(user))
                app = UserApp(name=user, score_dict=score_dict, data_dir='data_tables')
                main_logger.info("creating matches df for user: {}".format(user))
                matches_df = app.flat_viability()
                main_logger.info("saving matches for user: {}".format(user))
                matches_df.to_csv('results/{}_{}.csv'.format(user, datetime.today().strftime("%m_%d_%Y")))
            except Exception as e:
                main_logger.error("Error running UserApp: {}".format(e))
                raise e

    elif mode == 'single':
        try:
            main_logger.info("initiate SingleScoring object")
            scoring_manager = SingleScoring(data_dir='data_tables', user_name=user_name)
            main_logger.info("running scoring_manager")
            score_dict = scoring_manager.run_single_scoring()
        except Exception as e:
            main_logger.error("Error running SingleScoring: {}".format(e))
            raise e

        try:
            main_logger.info("initiate UserApp object for user: {}".format(user_name))
            app = UserApp(name=user_name, score_dict=score_dict, data_dir='data_tables')
            main_logger.info("creating matches df for user: {}".format(user_name))
            matches_df = app.add_contact_details()
            main_logger.info("saving matches for user: {}".format(user_name))
            csv_filename = 'results/{}_{}.csv'.format('single_test', datetime.today().strftime("%m_%d_%Y"))
            matches_df.to_csv(csv_filename)
            print(matches_df)
        except Exception as e:
            main_logger.error("Error running UserApp: {}".format(e))
            raise e

        try:
            main_logger.info("initiate EmailManager object for user: {}".format(user_name))
            mailer = EmailManager(name=user_name, csv_filename=csv_filename)
            # mailer.send_message()
            main_logger.info(" email sent for user: {}".format(user_name))
        except Exception as e:
            main_logger.error("Error running EmailManager: {}".format(e))
            raise e

if __name__ == '__main__':
    main(mode='single', user_name='Name 1')
