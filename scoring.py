import numpy as np
import pandas as pd
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics.pairwise import cosine_similarity
from logging_config import get_logger
from utils import set_indexes

pd.set_option('display.max_columns', None)

scoring_logger = get_logger("ark.scoring")


class Scoring:
    def __init__(self, data_dir):
        scoring_logger.info("starting __init__ for BulkScoring")
        self.bio_df = set_indexes(pd.read_csv('{}/bio_df.csv'.format(data_dir)))
        self.pref_df = set_indexes(pd.read_csv('{}/pref_df.csv'.format(data_dir)))
        self.int_df = set_indexes(pd.read_csv('{}/int_df.csv'.format(data_dir)))
        self.habit_df = set_indexes(pd.read_csv('{}/habit_df.csv'.format(data_dir)))
        self.pers_df = set_indexes(pd.read_csv('{}/pers_df.csv'.format(data_dir)))
        self.flat_df = set_indexes(pd.read_csv('{}/flat_df.csv'.format(data_dir)))
        self.extra_df = set_indexes(pd.read_csv('{}/extra_df.csv'.format(data_dir)))


    @staticmethod
    def flat_helper_function(flat_df):
        scoring_logger.info("starting flat_helper_function")
        try:
            # TODO: Make more efficient - hopefully scrap when moving away from google forms

            scoring_logger.info("separating strings by commas")

            flat_df["Flat: Where are you looking to live in London? (Tick all that apply)"] = \
                flat_df["Flat: Where are you looking to live in London? (Tick all that apply)"] \
                .str.split(',')

            flat_df["Flat: What price range are you looking for? Tick all that apply"] = \
                flat_df["Flat: What price range are you looking for? Tick all that apply"] \
                .str.split(',')

            scoring_logger.info("removing whitespaces and making list")

            flat_df["Flat: Where are you looking to live in London? (Tick all that apply)"] = \
                flat_df["Flat: Where are you looking to live in London? (Tick all that apply)"]. \
                apply(lambda x: [zone.lstrip(' ') for zone in x])

            flat_df["Flat: What price range are you looking for? Tick all that apply"] = \
                flat_df["Flat: What price range are you looking for? Tick all that apply"]. \
                apply(lambda x: [price_range.lstrip(' ') for price_range in x])

            scoring_logger.info("creating one_hot_encode_list function")

            def one_hot_encode_list(df, column_name):
                mlb = MultiLabelBinarizer()
                df = df.join(pd.DataFrame(mlb.fit_transform(df.pop(column_name)),
                                          columns=mlb.classes_,
                                          index=df.index))
                return df

            scoring_logger.info("applying one_hot_encode_list to relevant columns")

            flat_df = one_hot_encode_list(flat_df,
                                          "Flat: Where are you looking to live in London? (Tick all that apply)")

            flat_df = one_hot_encode_list(flat_df,
                                          "Flat: What price range are you looking for? Tick all that apply")

            scoring_logger.info("fixing column names")
            flat_df.columns = [column.strip() for column in flat_df.columns]

            scoring_logger.info("finishing flat_helper_function")
            return flat_df

        except Exception as e:
            scoring_logger.error("Error in habit_score: {}".format(e))
            raise e


class BulkScoring(Scoring):
    @staticmethod
    def bio_score(bio_df):
        scoring_logger.info("starting bio score")
        try:
            scoring_logger.info("splitting bio dfs")
            bio_filters_df = bio_df[['Bio: Gender',
                                     'Bio: Age',
                                     'Bio: Sexuality',
                                     'Bio: Birthplace',
                                     'Bio: Diet',
                                     'Bio: Ethnicity',
                                     'Bio: Political views',
                                     'Bio: Highest level of education',
                                     'Bio: Employment status']]
            print(bio_filters_df)
            bio_score_df = bio_df[['Bio: How often do you drink?',
                                   'Bio: How often do you smoke?',
                                   'Bio: Do you smoke/take weed?',
                                   'Bio: How often do you partake in any other recreational substances?']]

            scoring_logger.info("calculating bio similarity scores")
            bio_score_cos_matrix = pd.DataFrame(cosine_similarity(bio_score_df),
                                                index=bio_score_df.index,
                                                columns=bio_score_df.index)

            scoring_logger.info("finishing bio_score")
            return bio_score_cos_matrix, bio_filters_df
        except Exception as e:
            scoring_logger.error("Error in bio_score: {}".format(e))
            raise e

    @staticmethod
    def pref_score(pref_df, bio_filters_df):
        scoring_logger.info("starting  pref_score")
        try:
            scoring_logger.info("creating dummy variable for outer join and adding pref_name")
            pref_df["dummy"] = 0
            pref_df = pref_df.reset_index().rename(columns={"index": "pref_name"})

            scoring_logger.info("creating dummy variable for outer join and adding bio_name")
            bio_filters_df["dummy"] = 0
            bio_filters_df = bio_filters_df.reset_index().rename(columns={"index": "bio_name"})

            scoring_logger.info("merge pref and bio tables and fillna")
            merged_df = pd.merge(pref_df, bio_filters_df, on="dummy", how="outer")
            merged_df["Preferences: Minimum age of flatmate"] = merged_df["Preferences: Minimum age of flatmate"].fillna(18)
            merged_df["Preferences: Maximum age of flatmate"] = merged_df["Preferences: Maximum age of flatmate"].fillna(
                100)

            scoring_logger.info("remove matching pref and bio name rows, adding match column")
            merged_df = merged_df[merged_df["pref_name"] != merged_df["bio_name"]]
            merged_df["match"] = 0

            scoring_logger.info("creating pref_filter_match function")

            def pref_filter_match(row):
                if row["Preferences: Minimum age of flatmate"] <= row["Bio: Age"] \
                        <= row["Preferences: Maximum age of flatmate"]:
                    if row["Preferences: Sex of flatmate"] == "Open to all":
                        return 1
                    elif row["Bio: Gender"] in row["Preferences: Sex of flatmate"]:
                        return 1
                    else:
                        return 0
                else:
                    return 0

            scoring_logger.info("applying pref_filter_match function for match column")
            merged_df["match"] = merged_df.apply(lambda row: pref_filter_match(row), axis=1)
            merged_df = merged_df[["pref_name", "bio_name", "match"]]

            scoring_logger.info("re-merging dataframe to find symmetric matches")
            merged_df_double = pd.merge(merged_df,
                                        merged_df,
                                        left_on=["pref_name", "bio_name"],
                                        right_on=["bio_name", "pref_name"],
                                        how="inner")

            scoring_logger.info("remove self matching rows")
            merged_df_double = merged_df_double[merged_df_double["pref_name_x"] != merged_df_double["bio_name_x"]]

            scoring_logger.info("select symmetric matching rows")
            merged_df_double = merged_df_double[(merged_df_double["match_x"] == 1) & (merged_df_double["match_y"] == 1)]
            merged_df_double = merged_df_double[["pref_name_x", "pref_name_y"]]

            scoring_logger.info("finish pref_score")
            pref_matrix = merged_df_double[["pref_name_x", "pref_name_y"]]
            return pref_matrix

        except Exception as e:
            scoring_logger.error("Error in pref_score: {}".format(e))
            raise e

    @staticmethod
    def int_score(int_df):
        scoring_logger.info("starting int_score")
        try:
            scoring_logger.info("calculating  interest score")
            print(int_df)
            interest_score_cos_matrix = pd.DataFrame(cosine_similarity(int_df),
                                                     index=int_df.index,
                                                     columns=int_df.index)
            scoring_logger.info("finishing int_score")
            return interest_score_cos_matrix
        except Exception as e:
            scoring_logger.error("Error in int_score: {}".format(e))
            raise e

    @staticmethod
    def habit_score(habit_df):
        scoring_logger.info("starting habit_score")
        try:
            # One hot encodes string variables

            # groups columns by dtype and puts in dict
            scoring_logger.info("grouping columns by dtype and putting in dict")
            habit_df_dtype_dict = habit_df.columns.to_series().groupby(habit_df.dtypes).groups

            # selects list of text columns
            scoring_logger.info("creating list of string data type columns")
            habit_df_text_columns = list(habit_df_dtype_dict[np.dtype('O')])

            # creates df from one hot encoded text columns
            scoring_logger.info("create column of one hot encoded vectors from habit_df_text_columns")
            habit_object_df = pd.get_dummies(habit_df[habit_df_text_columns])

            # drops old text columns
            scoring_logger.info("dropping old columns")
            habit_df.drop(columns=habit_df_text_columns, inplace=True)

            # concats new one hot encoded columns
            scoring_logger.info("adding new columns")
            habit_df = pd.concat([habit_df, habit_object_df], axis=1)

            scoring_logger.info("calculating habit score")
            habit_score_cos_matrix = pd.DataFrame(cosine_similarity(habit_df),
                                                  index=habit_df.index,
                                                  columns=habit_df.index)

            scoring_logger.info("finishing habit_score")
            return habit_score_cos_matrix

        except Exception as e:
            scoring_logger.error("Error in habit_score: {}".format(e))
            raise e

    @staticmethod
    def pers_score(pers_df):
        scoring_logger.info("starting pers_score")
        try:
            scoring_logger.info("calculating personality score")
            personality_score_cos_matrix = pd.DataFrame(cosine_similarity(pers_df),
                                                        index=pers_df.index,
                                                        columns=pers_df.index)
            scoring_logger.info("finishing pers_score")
            return personality_score_cos_matrix
        except Exception as e:
            scoring_logger.error("Error in pers_score: {}".format(e))
            raise e

    def run_bulk_scoring(self):
        scoring_logger.info("starting run_data_manager")
        try:
            scoring_logger.info("running functions")

            bio_score_cos_matrix, bio_filters_df = self.bio_score(self.bio_df)
            pref_matrix = self.pref_score(self.pref_df, bio_filters_df)
            interest_score_cos_matrix = self.int_score(self.int_df)
            habit_score_cos_matrix = self.habit_score(self.habit_df)
            personality_score_cos_matrix = self.pers_score(self.pers_df)
            flat_df = self.flat_helper_function(self.flat_df)

            scoring_logger.info("creating score_dict")
            score_dict = {"user_info": bio_filters_df,
                          "bio_score_cos_matrix": bio_score_cos_matrix,
                          "pref_matrix": pref_matrix,
                          "interest_score_cos_matrix": interest_score_cos_matrix,
                          "habit_score_cos_matrix": habit_score_cos_matrix,
                          "personality_score_cos_matrix": personality_score_cos_matrix,
                          "flat_info": flat_df}

            scoring_logger.info("finishing run_data_manager")
            return score_dict
        except Exception as e:
            scoring_logger.error("Error in run_bulk_scoring: {}".format(e))
            raise e


class SingleScoring(Scoring):
    def __init__(self, data_dir, user_name):
        super(SingleScoring, self).__init__(data_dir)
        self.user_name = user_name

    def single_score(self, df, user_name):
        try:
            scoring_logger.info("calculating similarity scores")
            user_vector = df.loc[user_name].values.reshape(1, -1)
            reduced_df = df.drop(user_name, axis=0)
            reduced_index = reduced_df.index
            score_vector = cosine_similarity(user_vector, reduced_df)
            score_matrix = pd.DataFrame(score_vector.flatten(), index=reduced_index, columns=[user_name])
            return score_matrix
        except Exception as e:
            scoring_logger.error("Error in single_score: {}".format(e))
            raise e

    def bio_score(self, bio_df, user_name):
        scoring_logger.info("starting bio score")
        try:
            scoring_logger.info("splitting bio dfs")
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

            scoring_logger.info("calculating bio similarity scores")
            bio_score_cos_matrix = self.single_score(bio_score_df, user_name)
            scoring_logger.info("finishing bio_score")
            return bio_score_cos_matrix, bio_filters_df
        except Exception as e:
            scoring_logger.error("Error in bio_score: {}".format(e))
            raise e

    @staticmethod
    def pref_score(pref_df, bio_filters_df, user_name):
        scoring_logger.info("starting  pref_score")
        try:
            scoring_logger.info("creating dummy variable for outer join and adding pref_name")
            pref_df["dummy"] = 0
            pref_df = pref_df.reset_index().rename(columns={"index": "pref_name"})

            scoring_logger.info("creating dummy variable for outer join and adding bio_name")
            bio_filters_df["dummy"] = 0
            bio_filters_df = bio_filters_df.reset_index().rename(columns={"index": "bio_name"})

            scoring_logger.info("merge pref and bio tables and fillna")
            merged_df = pd.merge(pref_df, bio_filters_df, on="dummy", how="outer")
            merged_df["Preferences: Minimum age of flatmate"] = merged_df[
                "Preferences: Minimum age of flatmate"].fillna(18)
            merged_df["Preferences: Maximum age of flatmate"] = merged_df[
                "Preferences: Maximum age of flatmate"].fillna(
                100)

            scoring_logger.info("remove matching pref and bio name rows, adding match column")
            merged_df = merged_df[(merged_df["pref_name"] == user_name) | (merged_df["bio_name"] == user_name)]
            merged_df = merged_df[merged_df["pref_name"] != merged_df["bio_name"]]
            merged_df["match"] = 0

            scoring_logger.info("creating pref_filter_match function")

            def pref_filter_match(row):
                if row["Preferences: Minimum age of flatmate"] <= row["Bio: Age"] \
                        <= row["Preferences: Maximum age of flatmate"]:
                    if row["Preferences: Sex of flatmate"] == "Open to all":
                        return 1
                    elif row["Bio: Gender"] in row["Preferences: Sex of flatmate"]:
                        return 1
                    else:
                        return 0
                else:
                    return 0

            scoring_logger.info("applying pref_filter_match function for match column")
            merged_df["match"] = merged_df.apply(lambda row: pref_filter_match(row), axis=1)
            merged_df = merged_df[["pref_name", "bio_name", "match"]]

            scoring_logger.info("re-merging dataframe to find symmetric matches")
            merged_df_double = pd.merge(merged_df,
                                        merged_df,
                                        left_on=["pref_name", "bio_name"],
                                        right_on=["bio_name", "pref_name"],
                                        how="inner")

            scoring_logger.info("remove self matching rows")
            merged_df_double = merged_df_double[merged_df_double["pref_name_x"] != merged_df_double["bio_name_x"]]

            scoring_logger.info("select symmetric matching rows")
            merged_df_double = merged_df_double[(merged_df_double["match_x"] == 1) & (merged_df_double["match_y"] == 1)]
            merged_df_double = merged_df_double[["pref_name_x", "pref_name_y"]]

            scoring_logger.info("finish pref_score")
            pref_matrix = merged_df_double[["pref_name_x", "pref_name_y"]]
            pref_matrix = pref_matrix[pref_matrix.pref_name_x == user_name]
            return pref_matrix

        except Exception as e:
            scoring_logger.error("Error in pref_score: {}".format(e))
            raise e

    def int_score(self, int_df, user_name):
        scoring_logger.info("starting int_score")
        try:
            scoring_logger.info("calculating  interest score")
            interest_score_cos_matrix = self.single_score(int_df, user_name)
            scoring_logger.info("finishing int_score")
            return interest_score_cos_matrix
        except Exception as e:
            scoring_logger.error("Error in int_score: {}".format(e))
            raise e

    def habit_score(self, habit_df, user_name):
        scoring_logger.info("starting habit_score")
        try:
            # One hot encodes string variables

            # groups columns by dtype and puts in dict
            scoring_logger.info("grouping columns by dtype and putting in dict")
            habit_df_dtype_dict = habit_df.columns.to_series().groupby(habit_df.dtypes).groups

            # selects list of text columns
            scoring_logger.info("creating list of string data type columns")
            habit_df_text_columns = list(habit_df_dtype_dict[np.dtype('O')])

            # creates df from one hot encoded text columns
            scoring_logger.info("create column of one hot encoded vectors from habit_df_text_columns")
            habit_object_df = pd.get_dummies(habit_df[habit_df_text_columns])

            # drops old text columns
            scoring_logger.info("dropping old columns")
            habit_df.drop(columns=habit_df_text_columns, inplace=True)

            # concats new one hot encoded columns
            scoring_logger.info("adding new columns")
            habit_df = pd.concat([habit_df, habit_object_df], axis=1)

            scoring_logger.info("calculating habit score")
            habit_score_cos_matrix = self.single_score(habit_df, user_name)

            scoring_logger.info("finishing habit_score")
            return habit_score_cos_matrix

        except Exception as e:
            scoring_logger.error("Error in habit_score: {}".format(e))
            raise e

    def pers_score(self, pers_df, user_name):
        scoring_logger.info("starting pers_score")
        try:
            scoring_logger.info("calculating personality score")
            personality_score_cos_matrix = self.single_score(pers_df, user_name)
            scoring_logger.info("finishing pers_score")
            return personality_score_cos_matrix
        except Exception as e:
            scoring_logger.error("Error in pers_score: {}".format(e))
            raise e

    def run_single_scoring(self):
        scoring_logger.info("starting run_data_manager")
        try:
            scoring_logger.info("running functions")

            bio_score_cos_matrix, bio_filters_df = self.bio_score(self.bio_df, self.user_name)
            pref_matrix = self.pref_score(self.pref_df, bio_filters_df, self.user_name)
            interest_score_cos_matrix = self.int_score(self.int_df, self.user_name)
            habit_score_cos_matrix = self.habit_score(self.habit_df, self.user_name)
            personality_score_cos_matrix = self.pers_score(self.pers_df, self.user_name)
            flat_df = self.flat_helper_function(self.flat_df)

            scoring_logger.info("creating score_dict")
            score_dict = {"user_info": bio_filters_df,
                          "bio_score_cos_matrix": bio_score_cos_matrix,
                          "pref_matrix": pref_matrix,
                          "interest_score_cos_matrix": interest_score_cos_matrix,
                          "habit_score_cos_matrix": habit_score_cos_matrix,
                          "personality_score_cos_matrix": personality_score_cos_matrix,
                          "flat_info": flat_df}

            scoring_logger.info("finishing run_data_manager")
            return score_dict
        except Exception as e:
            scoring_logger.error("Error in run_single_scoring: {}".format(e))
            raise e
