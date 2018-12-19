import tweepy
import json

"""
Run this Program first thing should register Twitter developer
https://developer.twitter.com/
then Create APP
then generate consumer_key, consumer_secret, access_token, access_token_secret
put your key, secrets, token
"""
consumer_key = ''
consumer_secret = ''
access_token = ''
access_token_secret = ''

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

TOTAL_TWEETS = 200

# movie list and movie id list. they are match by 1 : 1
# such as movie_list[index]'s movie id is ate movie_id_list[index] where index = index in both variable
movie_list = ["Ghostbusters", "IndependenceDay", "PPZmovie", "warcraftmovie",
              "DreamWorksKFP3", "Triple9Movie", "AngryBirdMovie", "janegotagunfilm", "StarTrekMovie", "GodsofEgypt",
              "BatmanvSuperman",
              "sausage_party", "midnightspecial", "XMenMoviesUK", "legendoftarzan", "TheConjuring", "HowToBeSingle",
              "Divergent",
              "londonfallen", "BFGMovie", "HailCaesarMovie", "CaptainAmerica", "BenHurMovie", "TheJungleBook",
              "TheHuntsman", "NYSMmovie",
              "DirtyGrandpa", "deadpoolmovie", "disneypetes", "mebeforeyou", "SuicideSquadWB", "5thWaveMovie",
              "13hours", "tnd_film", "MBFGreekWedding", "CentralIntel", "MoneyMonster", "TMNTMovie", "TheWitchMovie",
              "GreenRoomMovie", "ffjmovie", "FreeStateMovie", "youngmessiahMOV", "EddieEagleMovie", "TheBoyMovie",
              "RideAlong", "TheBossFilm", "RaceMovie", "jasonbourne", "NeighborsMovie", "InfiltratorMov", "PetsMovie",
              "PlayNerve", "theforestisreal", "ZoolanderMovie", "ShallowsMovie", "10CloverfieldLn", "RisenMovie",
              "NatTurnerFilm", "MiraclesHeaven", "KeanuMovie", "LightsOutMovie", "GodsNotDeadFilm", "FSOBMovie",
              "AirliftFilm", "BadMoms", "MadMaxMovie", "HarvestTruth", "Terminator", "avengers2movie", "AntMan",
              "TheGoodDinosaur",
              "HungerGamesUK", "JurassicWorld", "PixarInsideOut", "CinderellaMovie", "_0_Tomorrowland", "HotelT2Game",
              "FantasticFour", "fast7Official", "MissionFilm", "Child44Movie", "NoEscapeMovie", "ChappieTheMovie",
              "blackhatmovie",
              "ManFromUNCLE", "PartTimeRogue", "Minions", "Ted2Official", "FiftyShades", "AlohaTheMovie",
              "AlmanacMovie", "PeanutsMovie",
              "Frankenstein", "Home_TheMovie", "SpongeBobMovie", "TheLongestRide", "SelflessFilm", "BusinessMovie",
              "TheBoyNextDoor",
              "RunAllNightFilm", "HotTubMovie", "lazarus_ind", "DiaryTheMovie", "WhileWereYoung", "everestmovie",
              "AssassinsMovie",
              "SanAndreasMovie", "PitchPerfQuotes", "BaahubaliMovie", "FocusMovieUK", "PaulBlartMovie", "LittleBoyFilm",
              "PointBreak",
              "gethardmovie", "TheInternMovie", "PixelsMovie", "GoosebumpsMovie", "BlackMassMovie", "RoomTheMovie",
              "ExMachinaMovie",
              "magicmikemovie", "GunmanThe", "panmovie", "hotpursuitmovie", "itfollowsfilm", "TrainwreckMovie",
              "TheDUFF", "thehatefuleight",
              "SicarioMovie", "Stonewall_Movie", "DaddysHome", "JoyTheMovie", "LastWitchHunter", "LegendTheFilm",
              "ComptonMovie", "UnfriendedMovie",
              "InsidiousMovie", "RevenantMovie", "SinisterMovie", "TheWalkMovie", "MartianMovie", "PaperTownsMovie",
              "KrampusDVD", "TheTransporter",
              "Anomalisamovie", "the33film", "AgeOfAdaline", "MazeRunnerMovie", "BurntMovie", "BridgeofSpies",
              "vacationmovie", "TheGallowsMovie",
              "WAYFMovie", "freeheldmovie", "SouthpawMovie", "DopeMovie", "creedmovie", "SpotlightMovie",
              "ByTheSeaMovie", "thebigshort",
              "SteveJobsFilm", "ConcussionMovie", "TheGiftMovie", "Captive", "lovethecoopers", "90minutesheaven",
              "miamericamovie"]

movie_id_list = [43074, 47933, 58431, 68735, 140300, 146198, 153518, 174751, 188927, 205584, 209112, 223702, 245703,
                 246655, 258489,
                 259693, 259694, 262504, 267860, 267935, 270487, 271110, 271969, 278927, 290595, 291805, 291870, 293660,
                 294272, 296096,
                 297761, 299687, 300671, 301365, 302688, 302699, 303858, 308531, 310131, 313922, 315664, 316152, 318850,
                 319888, 321258,
                 323675, 323676, 323677, 324668, 325133, 325789, 328111, 328387, 329440, 329833, 332567, 333371, 335778,
                 339408, 339984,
                 342521, 345911, 347126, 351819, 375290, 376659, 76341, 76757, 87101, 99861, 102899, 105864, 131634,
                 135397, 150540,
                 150689, 158852, 159824, 166424, 168259, 177677, 181283, 192141, 198184, 201088, 203801, 210860, 211672,
                 214756, 216015,
                 222936, 227719, 227973, 228066, 228161, 228165, 228205, 238615, 239573, 241251, 241554, 243938, 243940,
                 250124, 252512,
                 253412, 253450, 254128, 254470, 256040, 256591, 256961, 256962, 257088, 257091, 257211, 257344, 257445,
                 261023, 264644,
                 264660, 264999, 266396, 266647, 268920, 270303, 271718, 272693, 273248, 273481, 273899, 274167, 274479,
                 274854, 276907,
                 277216, 277685, 280092, 281957, 283445, 285783, 286217, 286565, 287903, 287948, 291270, 293646, 293863,
                 294254, 295964,
                 296098, 296099, 299245, 301351, 306745, 307081, 308639, 312221, 314365, 314385, 318846, 321697, 321741,
                 328425, 331190,
                 333348, 343795, 364083]

f = open("twitter_movie_data.json", "w")


def get_twitter_data():
    if len(movie_list) > 0:
        for target, movieId in zip(movie_list, movie_id_list):
            print("Geting data for " + target + " MovieID: " + str(movieId))
            item = api.user_timeline(target, count=TOTAL_TWEETS, include_rts=1)
            user = api.get_user(target)

            # scree_name is the name you can use @ symblo during using tweet
            # name is the real movie name
            screen_name = ""
            name = ""
            movie_id = movieId;

            tweets_favorite_count_mean = 0
            tweets_favorite_count_max = 0

            tweets_retweet_count_mean = 0
            tweets_retweet_count_max = 0

            # the statusJson will get a Tweet json object
            # by using format statusJson['Attribute'] to get informaiton
            for status in item:
                statusJson = status._json
                screen_name = statusJson['user']['screen_name']
                name = statusJson['user']['name']

                tweets_favorite_count_mean = tweets_favorite_count_mean + int(statusJson['favorite_count'])
                tweets_favorite_count_max = max(tweets_favorite_count_max, int(statusJson['favorite_count']))

                tweets_retweet_count_mean = tweets_retweet_count_mean + int(statusJson['retweet_count'])
                tweets_retweet_count_max = max(tweets_retweet_count_max, int(statusJson['retweet_count']))

            oneJson = {
                "oa_screen_name": screen_name,
                "movie_name": name,
                "offical_account_favorite_mean": tweets_favorite_count_mean / TOTAL_TWEETS,
                "offical_account_favorite_max": tweets_favorite_count_max,
                "offical_account_retweet_max": tweets_retweet_count_max,
                "oa_follower_count": user.followers_count,
                "offical_account_retweet_mean": tweets_retweet_count_mean / TOTAL_TWEETS,
                "movie_id": movie_id,
                "offical_account_favorite_sum": tweets_favorite_count_mean,
                "offical_account_retweet_sum": tweets_retweet_count_mean,
            }
            s = json.dumps(oneJson)
            f.write(s + "\n")

get_twitter_data()

f.close()