import csv
import time
from pyspark import Row

genresDic = {'Mystery': 14, 'Romance': 8, 'History': 15, 'Family': 6, 'Fantasy': 10, 'Horror': 16, 'Crime': 0,
             'Drama': 7, 'Science Fiction': 4, 'Animation': 5, 'Music': 9, 'Adventure': 2, 'Foreign': 18, 'Action': 3,
             'Comedy': 1, 'Documentary': 17, 'War': 12, 'Thriller': 11, 'Western': 13}

languageDic = {'en': 0, 'zh': 3, 'cn': 17, 'af': 9, 'vi': 20, 'is': 25, 'it': 6, 'xx': 22, 'id': 23, 'es': 2, 'ru': 12,
               'nl': 16, 'pt': 7, 'no': 18, 'nb': 21, 'th': 15, 'ro': 11, 'pl': 24, 'fr': 5, 'de': 1, 'da': 10,
               'fa': 19, 'hi': 13, 'ja': 4, 'he': 14, 'te': 26, 'ko': 8}

def is_valid_date(str):
    '''判断是否是一个有效的日期字符串'''
    try:
        time.strptime(str, "%m/%d/%y")
        return True
    except:
        return False

def clean_movie_data(line):

    dict = line.asDict()
    # print(dict)

    if('id' not in dict or dict['id'].isdigit() == False):
        return False
        # return Row(id = '9999999', original_language = 'en', revenue = 0, title = "error_data", budget = 0, release_date = '0000-00-00', genres = '[{"id": 99, "name": "error_data"}]')

    # original_language is in languageDict
    if('original_language' not in dict or languageDic.get(dict['original_language'], 99) == 99):
        return False

    # revenue is digit
    if('revenue' not in dict or dict['id'].isdigit() == False):
        return False

    # title, doesn't matter

    # budget is digit
    if('budget' not in dict or dict['budget'].isdigit() == False):
        return False

    # release_date, is a date
    if('release_date' not in dict or is_valid_date(dict['release_date']) == False):
        return False

    # genres, is in genresDic
    if('genres' not in dict):
        return False

    return True
