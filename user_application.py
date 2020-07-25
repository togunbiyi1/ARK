import os, sys
import pandas as pd
import numpy as np
from sklearn.preprocessing import MultiLabelBinarizer
from sklearn.metrics.pairwise import cosine_similarity
import scipy.sparse
pd.set_option('display.max_columns', None)


class UserApp:
    def __init__(self, name, score_dict):
        self.name = name
        self.user_info = score_dict["user_info"].loc[score_dict["user_info"].index == name]
        self.bio_score_cos_matrix = score_dict["bio_score_cos_matrix"][[self.name]]
        self.pref_matrix = score_dict["pref_matrix"][score_dict["pref_matrix"]["pref_name_x"] == self.name]
        self.interest_score_cos_matrix = score_dict["interest_score_cos_matrix"][[self.name]]
        self.habit_score_cos_matrix = score_dict["habit_score_cos_matrix"][[self.name]]
        self.personality_score_cos_matrix = score_dict["personality_score_cos_matrix"][[self.name]]
        self.flat_info = score_dict["flat_info"]

    def flat_df_match(self, flat_df, names_tuple):

        zone_filter_columns = ["Zone 1",
                               "Zone 2",
                               "Zone 3",
                               "Zone 4",
                               "Zone 5",
                               "Zone 6",
                               ]

        price_filter_columns = [
            "0-500",
            "500-599",
            "600-699",
            "700-799",
            "800-899",
            "900-999",
            "1000+"]

        temp_df = flat_df.loc[names_tuple]

        zone_list = []
        for zone in zone_filter_columns:
            if zone not in temp_df.columns:
                pass
            else:
                if temp_df.loc \
                        [[names_tuple[0]], zone].item() == temp_df.loc[[names_tuple[1]], zone].item():
                    zone_list.append(zone)
                else:
                    pass

        price_list = []
        for price in price_filter_columns:
            if price not in temp_df.columns:
                pass
            else:
                if temp_df.loc \
                        [[names_tuple[0]], price].item() == temp_df.loc[[names_tuple[1]], price].item():
                    price_list.append(price)
                else:
                    pass

        result_dict = {"zones": zone_list,
                       "prices": price_list}

        return result_dict

    def combine_scores(self):
        bio_score_cos_matrix = self.bio_score_cos_matrix
        pref_matrix = self.pref_matrix
        interest_score_cos_matrix = self.interest_score_cos_matrix
        habit_score_cos_matrix = self.habit_score_cos_matrix
        personality_score_cos_matrix = self.personality_score_cos_matrix

        bio_score_cos_matrix["score_type"] = "bio"
        interest_score_cos_matrix["score_type"] = "interest"
        habit_score_cos_matrix["score_type"] = "habit"
        personality_score_cos_matrix["score_type"] = "personality"

        combo_score = pd.concat([bio_score_cos_matrix,
                                 interest_score_cos_matrix,
                                 habit_score_cos_matrix,
                                 personality_score_cos_matrix],
                                axis=0)

        combo_score = combo_score.reset_index().rename(columns={"index": "match",
                                                                self.name: "score"})
        pref_matrix_list = list(pref_matrix["pref_name_y"])
        combo_score = combo_score[combo_score["match"].isin(pref_matrix_list)]
        combo_score = combo_score[combo_score["match"] != self.name].reset_index(drop=True)
        combo_score_average = combo_score.groupby("match").score.mean().reset_index()
        combo_score_average["score_type"] = "average"
        combo_score = pd.concat([combo_score_average, combo_score])

        combo_score_average["viable"] = 1

        return combo_score_average

    def flat_viability(self):
        combo_df = self.combine_scores()
        combo_df["zones"] = np.nan
        combo_df["prices"] = np.nan

        flat_info = self.flat_info
        matches = list(flat_info.index)
        matches.remove(self.name)
        for match in matches:
            flat_match_dict = self.flat_df_match(flat_info, [self.name, match])
            zones = flat_match_dict["zones"]
            prices = flat_match_dict["prices"]
            combo_df.loc[combo_df["match"] == match, "zones"] = {"zones": zones}
            combo_df.loc[combo_df["match"] == match, "prices"] = {"prices": prices}

            if len(zones) == 0 or len(prices) == 0:
                combo_df.loc[combo_df["match"] == match, "viable"] = 0

        combo_df = combo_df[combo_df["viable"] == 1]
        combo_df.sort_values(by=["score"], ascending=False, inplace=True)
        return combo_df.reset_index(drop=True)