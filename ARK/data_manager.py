import pandas as pd
from logging_config import get_logger

pd.set_option('display.max_columns', None)

data_manager_logger = get_logger("ark.data_manager")


class DataManager:
    def __init__(self, responses_filename):
        data_manager_logger.info("starting __init__ for DataManager")
        self.response_df = pd.read_csv(responses_filename)
        self.response_df.drop_duplicates(subset='Bio: Name', keep="last", inplace=True)
        self.response_df.set_index('Bio: Name', inplace=True)
        self.response_df.index.name = None
        timestamps = self.response_df['Timestamp']
        self.response_df.drop(columns=['Timestamp'], inplace=True)

    @staticmethod
    def sort_responses(df):

        data_manager_logger.info("starting sort_responses")
        try:
            data_manager_logger.info("creating empty question lists")
            bio_questions = []
            pref_questions = []
            int_questions = []
            hab_questions = []
            pers_questions = []
            flat_questions = []
            extra_questions = []
            contact_questions = []

            for column in df.columns:

                if "Bio" in column:
                    data_manager_logger.info("adding column: {} to Bio".format(column))
                    bio_questions.append(column)
                elif "Preferences" in column:
                    data_manager_logger.info("adding column: {} to Preferences".format(column))
                    pref_questions.append(column)
                elif "Interests" in column:
                    data_manager_logger.info("adding column: {} to Interests".format(column))
                    int_questions.append(column)
                elif "Habits" in column:
                    data_manager_logger.info("adding column: {} to Habits".format(column))
                    hab_questions.append(column)
                elif "Personality" in column:
                    data_manager_logger.info("adding column: {} to Personality".format(column))
                    pers_questions.append(column)
                elif "Flat" in column:
                    data_manager_logger.info("adding column: {} to Flat".format(column))
                    flat_questions.append(column)
                elif "Extra" in column:
                    data_manager_logger.info("adding column: {} to Extra".format(column))
                    extra_questions.append(column)
                elif "Contact" in column:
                    data_manager_logger.info("adding column: {} to Contact".format(column))
                    contact_questions.append(column)
                else:
                    print(column)

            data_manager_logger.info("creating dataframes")
            bio_df = df[bio_questions]
            pref_df = df[pref_questions]
            int_df = df[int_questions]
            habit_df = df[hab_questions]
            pers_df = df[pers_questions]
            flat_df = df[flat_questions]
            extra_df = df[extra_questions]
            contact_df = df[contact_questions]

            data_manager_logger.info("finishing sort_responses")

            return bio_df, pref_df, int_df, habit_df, pers_df, flat_df, extra_df, contact_df

        except Exception as e:
            data_manager_logger.error("Error in sort_responses: {}".format(e))
            raise e

    def save_tables(self):
        data_manager_logger.info("starting save_tables")
        try:
            data_manager_logger.info("calling sort_responses")
            bio_df, \
            pref_df, \
            int_df, \
            habit_df, \
            pers_df, \
            flat_df, \
            extra_df, \
            contact_df = self.sort_responses(self.response_df)

            data_manager_logger.info("saving bio_df")
            bio_df.to_csv('data_tables/bio_df.csv')

            data_manager_logger.info("saving pref_df")
            pref_df.to_csv('data_tables/pref_df.csv')

            data_manager_logger.info("saving int_df")
            int_df.to_csv('data_tables/int_df.csv')

            data_manager_logger.info("saving habit_df")
            habit_df.to_csv('data_tables/habit_df.csv')

            data_manager_logger.info("saving pers_df")
            pers_df.to_csv('data_tables/pers_df.csv')

            data_manager_logger.info("saving flat_df")
            flat_df.to_csv('data_tables/flat_df.csv')

            data_manager_logger.info("saving extra_df")
            extra_df.to_csv('data_tables/extra_df.csv')

            data_manager_logger.info("saving contact_df")
            contact_df.to_csv('data_tables/contact_df.csv')
        except Exception as e:
            data_manager_logger.error("Error in sort_responses: {}".format(e))
            raise e
