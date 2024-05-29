from enum import Enum


class Columns(Enum):
    HASHTAGS = 'hashtags'
    USER_NAME = 'user_name'
    USER_LOCATION = 'user_location'
    USER_VERIFIED = 'user_verified'
    USER_FOLLOWERS = 'user_followers'
    IS_RETWEET = 'is_retweet'
    SOURCE = 'source'
    TEXT = 'text'
    DATE = 'date'
