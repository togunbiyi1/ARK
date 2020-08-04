import pandas as pd
from datetime import datetime
from data_manager import DataManager
from scoring import BulkScoring, SingleScoring
from user_application import UserApp
import warnings
from logging_config import get_logger
import os, sys

directory_list = ['data_tables', 'log', 'responses', 'results']
for directory in directory_list:
    if not os.path.exists(directory):
        os.makedirs(directory)

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

main_logger = get_logger("ark.main")

if __name__ == '__main__':

    main_logger.info("*"*5 + " starting main ark application " + "*"*5)

    def main(mode, user_name=None):
        if mode == 'bulk':
            try:
                main_logger.info("initiate DataManager object")
                data_manager = DataManager(responses_filename='responses/ark_responses_3.csv')
                main_logger.info("running run_data_manager")
                data_manager.save_tables()

                main_logger.info("initiate BulkScoring object")
                scoring_manager = BulkScoring(data_dir='data_tables')
                main_logger.info("running scoring_manager")
                score_dict = scoring_manager.run_bulk_scoring()

                users = list(score_dict["user_info"].index)
            except Exception as e:
                main_logger.error("Error running DataManager or BulkScoring: {}".format(e))
                raise e

            for user in users:
                try:
                    main_logger.info("initiate UserApp object for user: {}".format(user))
                    app = UserApp(name=user, score_dict=score_dict)
                    main_logger.info("creating matches df for user: {}".format(user))
                    matches_df = app.flat_viability()
                    main_logger.info("saving matches for user: {}".format(user))
                    matches_df.to_csv('results/{}_{}.csv'.format(user, datetime.today().strftime("%m_%d_%Y")))
                    print(matches_df)
                except Exception as e:
                    main_logger.error("Error running UserApp: {}".format(e))
                    raise e

        elif mode == 'single':
            try:
                main_logger.info("initiate DataManager object")
                data_manager = DataManager(responses_filename='responses/ark_responses_3.csv')
                main_logger.info("running run_data_manager")
                data_manager.save_tables()

                main_logger.info("initiate BulkScoring object")
                scoring_manager = SingleScoring(data_dir='data_tables', user_name=user_name)
                main_logger.info("running scoring_manager")
                score_dict = scoring_manager.run_single_scoring()

                users = list(score_dict["user_info"].index)
            except Exception as e:
                main_logger.error("Error running DataManager or BulkScoring: {}".format(e))
                raise e

            try:
                main_logger.info("initiate UserApp object for user: {}".format(user_name))
                app = UserApp(name=user_name, score_dict=score_dict)
                main_logger.info("creating matches df for user: {}".format(user_name))
                matches_df = app.flat_viability()
                main_logger.info("saving matches for user: {}".format(user_name))
                matches_df.to_csv('results/{}_{}.csv'.format('single_test', datetime.today().strftime("%m_%d_%Y")))
                print(matches_df)
            except Exception as e:
                main_logger.error("Error running UserApp: {}".format(e))
                raise e

    main(mode='single', user_name='Tunji')
