import pandas as pd
import numpy as np
from logging_config import get_logger
from utils import set_indexes
pd.set_option('display.max_columns', None)

user_app_logger = get_logger("ark.user_app")

class UserApp:
    def __init__(self, name, score_dict, data_dir):
        user_app_logger.info("starting __init__ for UserApp")
        self.name = name
        self.user_info = score_dict["user_info"].loc[score_dict["user_info"].index == name]
        self.bio_score_cos_matrix = score_dict["bio_score_cos_matrix"][[self.name]]
        self.pref_matrix = score_dict["pref_matrix"][score_dict["pref_matrix"]["pref_name_x"] == self.name]
        self.interest_score_cos_matrix = score_dict["interest_score_cos_matrix"][[self.name]]
        self.habit_score_cos_matrix = score_dict["habit_score_cos_matrix"][[self.name]]
        self.personality_score_cos_matrix = score_dict["personality_score_cos_matrix"][[self.name]]
        self.flat_info = score_dict["flat_info"]
        self.contact_df = set_indexes(pd.read_csv('{}/contact_df.csv'.format(data_dir)))

    def flat_df_match(self, flat_df, names_tuple):
        user_app_logger.info("starting flat_df_match")
        try:
            # TODO Maybe move column to a config file

            user_app_logger.info("creating filter columns")
            zone_filter_columns = ["Edinburgh",
                                   "Zone 1",
                                   "Zone 2",
                                   "Zone 3",
                                   "Zone 4",
                                   "Zone 5",
                                   "Zone 6",
                                   "Oxford",
                                   "Southampton",
                                   ]

            price_filter_columns = [
                "0-500",
                "500-599",
                "600-699",
                "700-799",
                "800-899",
                "900-999",
                "1000+"]

            user_app_logger.info("selecting relevant rows")
            temp_df = flat_df.loc[names_tuple]

            zone_list = []
            for zone in zone_filter_columns:
                user_app_logger.info("check if zone: {} in temp_df".format(zone))
                if zone not in temp_df.columns:
                    user_app_logger.info("zone: {} not in temp_df".format(zone))
                    pass
                else:
                    user_app_logger.info("zone: {} in temp_df".format(zone))
                    if temp_df.loc \
                            [[names_tuple[0]], zone].item() == temp_df.loc[[names_tuple[1]], zone].item():
                        user_app_logger.info("add zone: {} to zone_list".format(zone))
                        zone_list.append(zone)
                    else:
                        user_app_logger.info("no match for zone: {}".format(zone))
                        pass

            price_list = []
            for price in price_filter_columns:
                user_app_logger.info("check if price: {} in temp_df".format(price))
                if price not in temp_df.columns:
                    user_app_logger.info("price: {} not in temp_df".format(price))
                    pass
                else:
                    if temp_df.loc \
                            [[names_tuple[0]], price].item() == temp_df.loc[[names_tuple[1]], price].item():
                        user_app_logger.info("add price: {} to price_list".format(price))
                        price_list.append(price)
                    else:
                        user_app_logger.info("no match for price: {}".format(price))
                        pass

            user_app_logger.info("creating result_dict")
            result_dict = {"zones": zone_list,
                           "prices": price_list}

            user_app_logger.info("finishing flat_df_match")
            return result_dict
        except Exception as e:
            user_app_logger.error("Error in flat_df_match: {}".format(e))
            raise e

    def combine_scores(self):
        user_app_logger.info("starting combine_scores")
        try:
            user_app_logger.info("gathering scores")
            bio_score_cos_matrix = self.bio_score_cos_matrix
            pref_matrix = self.pref_matrix
            interest_score_cos_matrix = self.interest_score_cos_matrix
            habit_score_cos_matrix = self.habit_score_cos_matrix
            personality_score_cos_matrix = self.personality_score_cos_matrix

            user_app_logger.info("adding df-specific columns")
            bio_score_cos_matrix["score_type"] = "bio"
            interest_score_cos_matrix["score_type"] = "interest"
            habit_score_cos_matrix["score_type"] = "habit"
            personality_score_cos_matrix["score_type"] = "personality"

            user_app_logger.info("concatenating score dfs")
            combo_score = pd.concat([bio_score_cos_matrix,
                                     interest_score_cos_matrix,
                                     habit_score_cos_matrix,
                                     personality_score_cos_matrix],
                                    axis=0)
            combo_score = combo_score.reset_index().rename(columns={"index": "match",
                                                                    self.name: "score"})

            user_app_logger.info("removing non-matching rows as per pref_matrix")
            pref_matrix_list = list(pref_matrix["pref_name_y"])
            combo_score = combo_score[combo_score["match"].isin(pref_matrix_list)]
            combo_score = combo_score[combo_score["match"] != self.name].reset_index(drop=True)
            combo_score_average = combo_score.groupby("match").score.mean().reset_index()

            combo_score_average["score_type"] = "average"
            combo_score_average["viable"] = 1

            user_app_logger.info("finishing combine_scores")
            return combo_score_average

        except Exception as e:
            user_app_logger.error("Error in combine_scores: {}".format(e))
            raise e

    def flat_viability(self):
        user_app_logger.info("starting flat_viability")
        try:
            user_app_logger.info("starting adding zones and prices columns")
            combo_df = self.combine_scores()
            combo_df["zones"] = np.nan
            combo_df["prices"] = np.nan

            user_app_logger.info("creating list of matches")
            flat_info = self.flat_info
            matches = list(flat_info.index)
            matches.remove(self.name)

            for match in matches:
                user_app_logger.info("running flat_df_match for {} and {}".format(self.name, match))
                flat_match_dict = self.flat_df_match(flat_info, [self.name, match])
                user_app_logger.info("adding zones and prices to combo_df")
                zones = flat_match_dict["zones"]
                prices = flat_match_dict["prices"]
                combo_df.loc[combo_df["match"] == match, "zones"] = {"zones": zones}
                combo_df.loc[combo_df["match"] == match, "prices"] = {"prices": prices}

                if len(zones) == 0 or len(prices) == 0:
                    user_app_logger.info("setting viable to 0 if no matches")
                    combo_df.loc[combo_df["match"] == match, "viable"] = 0

            user_app_logger.info("selecting only viable rows")
            combo_df = combo_df[combo_df["viable"] == 1]
            combo_df.sort_values(by=["score"], ascending=False, inplace=True)
            user_app_logger.info("finishing flat_viability")
            return combo_df.reset_index(drop=True)

        except Exception as e:
            user_app_logger.error("Error in flat_viability: {}".format(e))
            raise e

    def add_contact_details(self):
        try:
            user_app_logger.info('starting add_contact_details')
            combo_df = self.flat_viability()
            contact_df = self.contact_df.reset_index().rename(columns={'index': 'name'})
            user_app_logger.info('merging combo_df and contact_df')
            result_df = pd.merge(combo_df,
                                 contact_df,
                                 left_on="match",
                                 right_on="name",
                                 how='left').drop(columns=["name"])
            user_app_logger.info('finishing add_contact_details')
            return result_df
        except Exception as e:
            user_app_logger.error("Error in flat_viability: {}".format(e))
            raise e

