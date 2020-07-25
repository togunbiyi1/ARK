import pandas as pd
from datetime import datetime
from data_manager import DataManager
from user_application import UserApp
import warnings
from logging_config import get_logger

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

main_logger = get_logger("ark.main")

if __name__ == '__main__':

    main_logger.info("*"*5 + " starting main ark application " "*"*5)
    try:
        main_logger.info("initiate DataManager object")
        run_through = DataManager(responses_filename='responses/ark_responses_2.csv')
        main_logger.info("running run_data_manager")
        score_dict = run_through.run_data_manager()

        users = list(score_dict["user_info"].index)
    except Exception as e:
        main_logger.error("Error running DataManager: {}".format(e))
        raise e

    for user in users:
        try:
            main_logger.info("initiate UserApp object for user: {}".format(user))
            app = UserApp(name=user, score_dict=score_dict)
            main_logger.info("creating matches df for user: {}".format(user))
            matches_df = app.flat_viability()
            main_logger.info("saving matches for user: {}".format(user))
            matches_df.to_csv('results/{}_{}.csv'.format(user, datetime.today().strftime("%m_%d_%Y")))
        except Exception as e:
            main_logger.error("Error running UserApp: {}".format(e))
            raise e

