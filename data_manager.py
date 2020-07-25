import os, sys
import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics.pairwise import cosine_similarity
import scipy.sparse
pd.set_option('display.max_columns', None)


class DataManager:
    def __init__(self, responses_filename):
        self.response_df = pd.read_csv(responses_filename)
        self.response_df.set_index('Bio: Name', inplace=True)
        self.response_df.index.name = None
        timestamps = self.response_df['Timestamp']
        self.response_df.drop(columns=['Timestamp'], inplace=True)

    def sort_responses(self, df):
        bio_questions = []
        pref_questions = []
        int_questions = []
        hab_questions = []
        pers_questions = []
        flat_questions = []
        extra_questions = []

        for column in df.columns:
            if "Bio" in column:
                bio_questions.append(column)
            elif "Preferences" in column:
                pref_questions.append(column)
            elif "Interests" in column:
                int_questions.append(column)
            elif "Habits" in column:
                hab_questions.append(column)
            elif "Personality" in column:
                pers_questions.append(column)
            elif "Flat" in column:
                flat_questions.append(column)
            elif "Extra" in column:
                extra_questions.append(column)
            else:
                print(column)

        bio_df = df[bio_questions]
        pref_df = df[pref_questions]
        int_df = df[int_questions]
        habit_df = df[hab_questions]
        pers_df = df[pers_questions]
        flat_df = df[flat_questions]
        extra_df = df[extra_questions]

        return bio_df, pref_df, int_df, habit_df, pers_df, flat_df, extra_df

    def bio_score(self, bio_df):

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

        bio_score_cos_matrix = pd.DataFrame(cosine_similarity(bio_score_df),
                                            index=bio_score_df.index,
                                            columns=bio_score_df.index)

        return bio_score_cos_matrix, bio_filters_df

    def pref_score(self, pref_df, bio_filters_df):

        pref_df["dummy"] = 0
        pref_df = pref_df.reset_index().rename(columns={"index": "pref_name"})
        bio_filters_df["dummy"] = 0
        bio_filters_df = bio_filters_df.reset_index().rename(columns={"index": "bio_name"})

        merged_df = pd.merge(pref_df, bio_filters_df, on="dummy", how="outer")
        merged_df["Preferences: Minimum age of flatmate"] = merged_df["Preferences: Minimum age of flatmate"].fillna(18)
        merged_df["Preferences: Maximum age of flatmate"] = merged_df["Preferences: Maximum age of flatmate"].fillna(
            100)

        merged_df = merged_df[merged_df["pref_name"] != merged_df["bio_name"]]
        merged_df["match"] = 0

        def pref_filter(row):
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

        merged_df["match"] = merged_df.apply(lambda row: pref_filter(row), axis=1)
        merged_df = merged_df[["pref_name", "bio_name", "match"]]

        merged_df_double = pd.merge(merged_df,
                                    merged_df,
                                    left_on=["pref_name", "bio_name"],
                                    right_on=["bio_name", "pref_name"],
                                    how="inner")

        merged_df_double = merged_df_double[merged_df_double["pref_name_x"] != merged_df_double["bio_name_x"]]

        merged_df_double = merged_df_double[(merged_df_double["match_x"] == 1) & (merged_df_double["match_y"] == 1)]
        merged_df_double = merged_df_double[["pref_name_x", "pref_name_y"]]

        pref_matrix = merged_df_double[["pref_name_x", "pref_name_y"]]

        return pref_matrix

    def int_score(self, int_df):

        interest_score_cos_matrix = pd.DataFrame(cosine_similarity(int_df),
                                                 index=int_df.index,
                                                 columns=int_df.index)

        return interest_score_cos_matrix

    def habit_score(self, habit_df):

        # One hot encodes string variables

        # groups columns by dtype and puts in dict
        habit_df_dtype_dict = habit_df.columns.to_series().groupby(habit_df.dtypes).groups

        # selects list of text columns
        habit_df_text_columns = list(habit_df_dtype_dict[np.dtype('O')])

        # creates df from one hot encoded text columns
        habit_object_df = pd.get_dummies(habit_df[habit_df_text_columns])

        # drops old text columns
        habit_df.drop(columns=habit_df_text_columns, inplace=True)

        # concats new one hot encoded columns
        habit_df = pd.concat([habit_df, habit_object_df], axis=1)

        habit_score_cos_matrix = pd.DataFrame(cosine_similarity(habit_df),
                                              index=habit_df.index,
                                              columns=habit_df.index)

        return habit_score_cos_matrix

    def pers_score(self, pers_df):

        personality_score_cos_matrix = pd.DataFrame(cosine_similarity(pers_df),
                                                    index=pers_df.index,
                                                    columns=pers_df.index)

        return personality_score_cos_matrix

    def flat_score(self, flat_df):

        # TODO: Make more efficient
        flat_df["Flat: Where are you looking to live in London? (Tick all that apply)"] = \
            flat_df["Flat: Where are you looking to live in London? (Tick all that apply)"] \
                .str.split(',')

        flat_df["Flat: What price range are you looking for? Tick all that apply"] = \
            flat_df["Flat: What price range are you looking for? Tick all that apply"] \
                .str.split(',')

        flat_df["Flat: Where are you looking to live in London? (Tick all that apply)"] = \
            flat_df["Flat: Where are you looking to live in London? (Tick all that apply)"]. \
                apply(lambda x: [zone.lstrip(' ') for zone in x])

        flat_df["Flat: What price range are you looking for? Tick all that apply"] = \
            flat_df["Flat: What price range are you looking for? Tick all that apply"]. \
                apply(lambda x: [price_range.lstrip(' ') for price_range in x])

        def one_hot_encode_list(df, column_name):
            mlb = MultiLabelBinarizer()
            df = df.join(pd.DataFrame(mlb.fit_transform(df.pop(column_name)),
                                      columns=mlb.classes_,
                                      index=df.index))
            return df

        flat_df = one_hot_encode_list(flat_df,
                                      "Flat: Where are you looking to live in London? (Tick all that apply)")

        flat_df = one_hot_encode_list(flat_df,
                                      "Flat: What price range are you looking for? Tick all that apply")

        flat_df.columns = [column.strip() for column in flat_df.columns]

        return flat_df

    def run_data_manager(self):

        bio_df, pref_df, int_df, habit_df, pers_df, flat_df, extra_df = self.sort_responses(self.response_df)

        bio_score_cos_matrix, bio_filters_df = self.bio_score(bio_df)
        pref_matrix = self.pref_score(pref_df, bio_filters_df)
        interest_score_cos_matrix = self.int_score(int_df)
        habit_score_cos_matrix = self.habit_score(habit_df)
        personality_score_cos_matrix = self.pers_score(pers_df)
        flat_df = self.flat_score(flat_df)

        score_dict = {"user_info": bio_filters_df,
                      "bio_score_cos_matrix": bio_score_cos_matrix,
                      "pref_matrix": pref_matrix,
                      "interest_score_cos_matrix": interest_score_cos_matrix,
                      "habit_score_cos_matrix": habit_score_cos_matrix,
                      "personality_score_cos_matrix": personality_score_cos_matrix,
                      "flat_info": flat_df}

        return score_dict
