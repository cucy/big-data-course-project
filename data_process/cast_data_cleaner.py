def clean_cast_data(line):
    dict = line.asDict()
    # print(dict)
    if('movie_id' not in dict):
        return False

    if('cast' not in dict):
        return False

    return True

