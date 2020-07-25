import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics.pairwise import cosine_similarity
from logging_config import get_logger

pd.set_option('display.max_columns', None)

data_manager_logger = get_logger("ark.data_manager")

class DataManager:
    def __init__(self, responses_filename):
        data_manager_logger.info("starting __init__ for DataManager")
        self.response_df = pd.read_csv(responses_filename)
        self.response_df.set_index('Bio: Name', inplace=True)
        self.response_df.index.name = None
        timestamps = self.response_df['Timestamp']
        self.response_df.drop(columns=['Timestamp'], inplace=True)

    def sort_responses(self, df):

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

            for column in df.columns:

                if "Bio" in column:
                    data_manager_logger.info("adding column: {} to Bio")
                    bio_questions.append(column)
                elif "Preferences" in column:
                    data_manager_logger.info("adding column: {} to Preferences")
                    pref_questions.append(column)
                elif "Interests" in column:
                    data_manager_logger.info("adding column: {} to Interests")
                    int_questions.append(column)
                elif "Habits" in column:
                    data_manager_logger.info("adding column: {} to Habits")
                    hab_questions.append(column)
                elif "Personality" in column:
                    data_manager_logger.info("adding column: {} to Personality")
                    pers_questions.append(column)
                elif "Flat" in column:
                    data_manager_logger.info("adding column: {} to Flat")
                    flat_questions.append(column)
                elif "Extra" in column:
                    data_manager_logger.info("adding column: {} to Extra")
                    extra_questions.append(column)
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

            data_manager_logger.info("finishing sort_responses")

            return bio_df, pref_df, int_df, habit_df, pers_df, flat_df, extra_df

        except Exception as e:
            data_manager_logger.error("Error in sort_responses: {}".format(e))
            raise e

    def bio_score(self, bio_df):
        data_manager_logger.info("starting bio score")
        try:
            data_manager_logger.info("splitting bio dfs")
            bio_filters_df = bio_df[['Bio: Gender',
                                     'Bio: Age',
                                     'Bio: Sexuality',
                                     'Bio: Birthplace',
                                     'Bio: Diet',
                                     'Bio: Ethnicity',
                                     'Bio: Political views',
                                     'Bio: Highest level of education',
                                     'Bio: Employment status']]

            bio_score_df = bio_df[['Bio: How often do you drink?',
                                   'Bio: How often do you smoke?',
                                   'Bio: Do you smoke/take weed?',
                                   'Bio: How often do you partake in any other recreational substances?']]

            data_manager_logger.info("calculating bio similarity scores")
            bio_score_cos_matrix = pd.DataFrame(cosine_similarity(bio_score_df),
                                                index=bio_score_df.index,
                                                columns=bio_score_df.index)

            data_manager_logger.info("finishing bio_score")
            return bio_score_cos_matrix, bio_filters_df
        except Exception as e:
            data_manager_logger.error("Error in bio_score: {}".format(e))
            raise e

    def pref_score(self, pref_df, bio_filters_df):
        data_manager_logger.info("starting  pref_score")
        try:
            data_manager_logger.info("creating dummy variable for outer join and adding pref_name")
            pref_df["dummy"] = 0
            pref_df = pref_df.reset_index().rename(columns={"index": "pref_name"})

            data_manager_logger.info("creating dummy variable for outer join and adding bio_name")
            bio_filters_df["dummy"] = 0
            bio_filters_df = bio_filters_df.reset_index().rename(columns={"index": "bio_name"})

            data_manager_logger.info("merge pref and bio tables and fillna")
            merged_df = pd.merge(pref_df, bio_filters_df, on="dummy", how="outer")
            merged_df["Preferences: Minimum age of flatmate"] = merged_df["Preferences: Minimum age of flatmate"].fillna(18)
            merged_df["Preferences: Maximum age of flatmate"] = merged_df["Preferences: Maximum age of flatmate"].fillna(
                100)

            data_manager_logger.info("remove matching pref and bio name rows, adding match column")
            merged_df = merged_df[merged_df["pref_name"] != merged_df["bio_name"]]
            merged_df["match"] = 0

            data_manager_logger.info("creating pref_filter_match function")
            def pref_filter_match(row):
                if row["Bio: Age"] >= row["Preferences: Minimum age of flatmate"] and \
                                row["Bio: Age"] <= row["Preferences: Maximum age of flatmate"]:
                    if row["Preferences: Sex of flatmate"] == "Open to all":
                        return 1
                    elif row["Bio: Gender"] in row["Preferences: Sex of flatmate"]:
                        return 1
                    else:
                        return 0
                else:
                    return 0

            data_manager_logger.info("applying pref_filter_match function for match column")
            merged_df["match"] = merged_df.apply(lambda row: pref_filter_match(row), axis=1)
            merged_df = merged_df[["pref_name", "bio_name", "match"]]

            data_manager_logger.info("remerging dataframe to find symmetric matches")
            merged_df_double = pd.merge(merged_df,
                                        merged_df,
                                        left_on=["pref_name", "bio_name"],
                                        right_on=["bio_name", "pref_name"],
                                        how="inner")

            data_manager_logger.info("remove self matching rows")
            merged_df_double = merged_df_double[merged_df_double["pref_name_x"] != merged_df_double["bio_name_x"]]

            data_manager_logger.info("select symmetric matching rows")
            merged_df_double = merged_df_double[(merged_df_double["match_x"] == 1) & (merged_df_double["match_y"] == 1)]
            merged_df_double = merged_df_double[["pref_name_x", "pref_name_y"]]

            data_manager_logger.info("finish pref_score")
            pref_matrix = merged_df_double[["pref_name_x", "pref_name_y"]]
            return pref_matrix

        except Exception as e:
            data_manager_logger.error("Error in pref_score: {}".format(e))
            raise e

    def int_score(self, int_df):
        data_manager_logger.info("starting int_score")
        try:
            data_manager_logger.info("calculating  interest score")
            interest_score_cos_matrix = pd.DataFrame(cosine_similarity(int_df),
                                                 index=int_df.index,
                                                 columns=int_df.index)
            data_manager_logger.info("finishing int_score")
            return interest_score_cos_matrix
        except Exception as e:
            data_manager_logger.error("Error in int_score: {}".format(e))
            raise e


    def habit_score(self, habit_df):
        data_manager_logger.info("starting habit_score")
        try:
            # One hot encodes string variables

            # groups columns by dtype and puts in dict
            data_manager_logger.info("grouping columns by dtype and putting in dict")
            habit_df_dtype_dict = habit_df.columns.to_series().groupby(habit_df.dtypes).groups

            # selects list of text columns
            data_manager_logger.info("creating list of string data type columns")
            habit_df_text_columns = list(habit_df_dtype_dict[np.dtype('O')])

            # creates df from one hot encoded text columns
            data_manager_logger.info("create column of one hot encoded vectors from habit_df_text_columns")
            habit_object_df = pd.get_dummies(habit_df[habit_df_text_columns])

            # drops old text columns
            data_manager_logger.info("dropping old columns")
            habit_df.drop(columns=habit_df_text_columns, inplace=True)

            # concats new one hot encoded columns
            data_manager_logger.info("adding new columns")
            habit_df = pd.concat([habit_df, habit_object_df], axis=1)

            data_manager_logger.info("calculating habit score")
            habit_score_cos_matrix = pd.DataFrame(cosine_similarity(habit_df),
                                                  index=habit_df.index,
                                                  columns=habit_df.index)

            data_manager_logger.info("finishing habit_score")
            return habit_score_cos_matrix

        except Exception as e:
            data_manager_logger.error("Error in habit_score: {}".format(e))
            raise e

    def pers_score(self, pers_df):
        data_manager_logger.info("starting pers_score")
        try:
            data_manager_logger.info("calculating personality score")
            personality_score_cos_matrix = pd.DataFrame(cosine_similarity(pers_df),
                                                        index=pers_df.index,
                                                        columns=pers_df.index)
            data_manager_logger.info("finishing pers_score")
            return personality_score_cos_matrix
        except Exception as e:
            data_manager_logger.error("Error in pers_score: {}".format(e))
            raise e

    def flat_helper_function(self, flat_df):
        data_manager_logger.info("starting flat_helper_function")
        try:
            # TODO: Make more efficient - hopefully scrap when moving away from google forms

            data_manager_logger.info("separating strings by commas")

            flat_df["Flat: Where are you looking to live in London? (Tick all that apply)"] = \
                flat_df["Flat: Where are you looking to live in London? (Tick all that apply)"] \
                    .str.split(',')

            flat_df["Flat: What price range are you looking for? Tick all that apply"] = \
                flat_df["Flat: What price range are you looking for? Tick all that apply"] \
                    .str.split(',')

            data_manager_logger.info("removing whitespaces and making list")

            flat_df["Flat: Where are you looking to live in London? (Tick all that apply)"] = \
                flat_df["Flat: Where are you looking to live in London? (Tick all that apply)"]. \
                    apply(lambda x: [zone.lstrip(' ') for zone in x])

            flat_df["Flat: What price range are you looking for? Tick all that apply"] = \
                flat_df["Flat: What price range are you looking for? Tick all that apply"]. \
                    apply(lambda x: [price_range.lstrip(' ') for price_range in x])

            data_manager_logger.info("creating one_hot_encode_list function")

            def one_hot_encode_list(df, column_name):
                mlb = MultiLabelBinarizer()
                df = df.join(pd.DataFrame(mlb.fit_transform(df.pop(column_name)),
                                          columns=mlb.classes_,
                                          index=df.index))
                return df

            data_manager_logger.info("applying one_hot_encode_list to relevant columns")

            flat_df = one_hot_encode_list(flat_df,
                                          "Flat: Where are you looking to live in London? (Tick all that apply)")

            flat_df = one_hot_encode_list(flat_df,
                                          "Flat: What price range are you looking for? Tick all that apply")

            data_manager_logger.info("fixing column names")
            flat_df.columns = [column.strip() for column in flat_df.columns]

            data_manager_logger.info("finishing flat_helper_function")
            return flat_df

        except Exception as e:
            data_manager_logger.error("Error in habit_score: {}".format(e))
            raise e

    def run_data_manager(self):

        data_manager_logger.info("starting run_data_manager")
        try:
            data_manager_logger.info("running functions")
            bio_df, pref_df, int_df, habit_df, pers_df, flat_df, extra_df = self.sort_responses(self.response_df)

            bio_score_cos_matrix, bio_filters_df = self.bio_score(bio_df)
            pref_matrix = self.pref_score(pref_df, bio_filters_df)
            interest_score_cos_matrix = self.int_score(int_df)
            habit_score_cos_matrix = self.habit_score(habit_df)
            personality_score_cos_matrix = self.pers_score(pers_df)
            flat_df = self.flat_helper_function(flat_df)

            data_manager_logger.info("creating score_dict")
            score_dict = {"user_info": bio_filters_df,
                          "bio_score_cos_matrix": bio_score_cos_matrix,
                          "pref_matrix": pref_matrix,
                          "interest_score_cos_matrix": interest_score_cos_matrix,
                          "habit_score_cos_matrix": habit_score_cos_matrix,
                          "personality_score_cos_matrix": personality_score_cos_matrix,
                          "flat_info": flat_df}

            data_manager_logger.info("finishing run_data_manager")
            return score_dict
        except Exception as e:
            data_manager_logger.error("Error in run_data_manager: {}".format(e))
            raise e
