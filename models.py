import uuid
import pandas as pd

class User:

    def __init__(self,
                 name,
                 email,
                 password,
                 id=None,
                 profile_picture=None,
                 pictures=[],
                 locations=[],
                 min_price=None,
                 max_price=None,
                 male_pref=True,
                 female_pref=True,
                 min_age=18,
                 max_age=100,
                 room_price=0,
                 room_type="Double",
                 ensuite=False,
                 balcony=False,
                 prop_type="Apartment",
                 new_build=False,
                 concierge=False,
                 garden=False,
                 parking=False,
                 living_room=True,
                 lift=False,
                 bathtub=False,
                 social_index=5,
                 overnight_index=5,
                 party_index=5,
                 cleanliness_index=5,
                 noise_index=5,
                 share_index=5,
                 relationship_index=5,
                 drug_index=5):
        """
        id: unique identifier
        name: str
        email: str
        password: str
        profile_picture: str of link to picture
        pictures: list of strs of links to pictures
        property_id: unique identifier for property
        room_user_dict: dict of of form {room_id:user_id}
        locations: list of str of locations
        """
        if id:
            self.id = id
        else:
            self.id = uuid.uuid4()
        self.name = name
        self.email = email
        self.password = password
        self.profile_picture = profile_picture
        self.pictures = pictures
        self.locations = locations
        self.min_price = min_price
        self.max_price = max_price
        self.male_pref = male_pref
        self.female_pref = female_pref
        self.min_age = min_age
        self.max_age = max_age
        self.room_price = room_price
        self.room_type = room_type
        self.ensuite = ensuite
        self.balcony = balcony
        self.prop_type = prop_type
        self.new_build = new_build
        self.concierge = concierge
        self.garden = garden
        self.parking = parking
        self.living_room = living_room
        self.lift = lift
        self.bathtub = bathtub
        self.social_index = social_index
        self.overnight_index = overnight_index
        self.party_index = party_index
        self.cleanliness_index = cleanliness_index
        self.noise_index = noise_index
        self.share_index = share_index
        self.relationship_index = relationship_index
        self.drug_index = drug_index

    def change_name(self, new_name):
        self.name = new_name

    def change_email(self, new_email):
        self.email = new_email

    def change_password(self, new_password):
        self.password = new_password

    def change_profile_picture(self, new_picture):
        self.profile_picture = new_picture

    def remove_picture(self, old_picture):
        if old_picture in self.pictures:
            self.pictures.remove(old_picture)
        else:
            print("picture doesn't exist")

    def add_picture(self, new_picture):
        if new_picture not in self.pictures:
            self.pictures.append(new_picture)
        else:
            print("picture already added")

    def add_location(self, location):
        self.locations[location] = True

    def remove_location(self, location):
        del self.locations[location]

    def set_price_range(self, min, max):
        if min < 0 or max < 0:
            print("can't have negative price")
        self.min_price = min
        self.max_price = max

    def set_gender_pref(self, male_pref, female_pref):
        self.male_pref = male_pref
        self.male_pref = female_pref

    def set_age_range(self, min, max):
        self.min_age = min
        self.max_age = max

    def set_social_index(self, social_score):
        self.social_index = social_score

    def set_overnight_index(self, overnight_score):
        self.overnight_index = overnight_score

    def set_party_index(self, party_score):
        self.party_index = party_score

    def set_cleanliness_index(self, cleanliness_score):
        self.cleanliness_index = cleanliness_score

    def set_noise_index(self, noise_score):
        self.noise_index = noise_score

    def set_share_index(self, share_score):
        self.share_index = share_score

    def set_relationship_index(self, relationship_score):
        self.relationship_index = relationship_score

    def set_drug_index(self, drug_score):
        self.drug_index = drug_score

    def create_user_table_row_dict(self):
        data = {"id":self.id,
                "name":self.name,
                "email":self.email,
                "password":self.password,
                "profile_picture": self.profile_picture,
                "pictures": self.pictures,
                "locations": self.locations,
                "min_price":self.min_price,
                "max_price":self.max_price,
                "male_pref":self.male_pref,
                "female_pref":self.female_pref,
                "min_age":self.min_age,
                "max_age":self.max_age,
                "social_index":self.social_index,
                "overnight_index":self.overnight_index,
                "party_index":self.party_index,
                "cleanliness_index":self.cleanliness_index,
                "noise_index":self.noise_index,
                "share_index":self.share_index,
                "relationship_index":self.relationship_index,
                "drug_index":self.drug_index}
        return data

    def update_user_table_row(self, df):
        user_table_row_dict = self.create_user_table_row_dict()
        if self.id in df["id"]:
            for column in df.columns:
                if column == "id":
                    continue
                else:
                    df.loc[df["id"] == self.id, column] = user_table_row_dict[column]
        else:
            df.append(pd.DataFrame(user_table_row_dict)).reset_index(drop=True)


class Room:

    def __init__(self, property_id, price, type="Double", ensuite=False, balcony=False):
        self.id = uuid.uuid4()
        self.property_id = property_id
        self.price = price
        self.type = type
        self.ensuite = ensuite
        self.balcony = balcony

    def set_price(self, price):
        self.price = price

    def set_type(self, type):
        self.type = type

    def set_ensuite(self, ensuite):
        self.ensuite = ensuite

    def set_balcony(self, balcony):
        self.balcony = balcony


class Property:

    def __init__(self, location, type, new_build, concierge, garden, parking, living_room, lift, bathtub):
        self.id = uuid.uuid4()
        self.location = location
        self.type = type
        self.new_build = new_build
        self.concierge = concierge
        self.garden = garden
        self.parking = parking
        self.living_room = living_room
        self.lift = lift
        self.bathtub = bathtub

    def set_location(self, location):
        self.location = location

    def set_type(self, type):
        self.type = type

    def set_build(self, new_build):
        self.new_build = new_build

    def set_concierge(self, concierge):
        self.concierge = concierge

    def set_garden(self, garden):
        self.garden = garden

    def set_parking(self, parking):
        self.parking = parking

    def set_living_room(self, living_room):
        self.living_room = living_room

    def set_lift(self, lift):
        self.lift = lift

    def set_bathtub(self, bathtub):
        self.bathtub = bathtub


class PropertyMatch:

    def __init__(self, property, room_user_dict):
        """
        id: unique identifier
        property_id: unique identifier for property
        room_user_dict: dict of of form {room_id:user_id}
         """
        self.id = uuid.uuid4()
        self.property_id = property
        self.room_user_dict = room_user_dict
        self.users = self.room_user_dict.values()
        self.user_acceptance_dict = {user: "pending" for user in self.users}


