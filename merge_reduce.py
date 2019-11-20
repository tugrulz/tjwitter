'''
Some additional scripts to organize files:
merge_reduce(): merge multiple .csv's and then drop duplicates using access field
split_metadata(): self explanatory, in case tweet_metadata.csv is too big
'''

import os
import pandas as pd
import argparse

def split_metadata(timeline = True):
    if(timeline):
        fields_metadata = ["id", "author_id", "created_at", "lang",
                       "tweet_type",
                       "retweet_count", "favorite_count",
                       "sensitive", "access"]
    else:
        fields_metadata = ["id", "author_id", "created_at", "lang",
                       "tweet_type",
                       "retweet_count", "favorite_count", "quote_count", "reply_count",
                       "sensitive", "access"]

    fields_network = ["id", "author_id", "created_at", "lang",
                       "tweet_type",
                       "linked_tweet", "linked_user", "access"]

    fields_source = ["id", "author_id", "created_at", "lang",
                       "tweet_type", "source", "source_link"]

    df = pd.read_csv('./merged_tables/tweet_metadata.csv', keep_default_na= False, usecols=fields_metadata)
    df.to_csv('./merged_tables/tweet_metadata_compact.csv', index = False)

    df = pd.read_csv('./merged_tables/tweet_metadata.csv', keep_default_na= False, usecols=fields_network)
    df.to_csv('./merged_tables/tweet_metadata_network.csv', index = False)

    df = pd.read_csv('./merged_tables/tweet_metadata.csv', keep_default_na=False, usecols = fields_source)
    df.to_csv('./merged_tables/tweet_source.csv', index = False)


def drop_duplicates(folder, skip = []):
    print("Dropping duplicates")

    files = os.listdir(folder)


    # keep last from user, tweet metadata, sensitive, user_timezone according to id
    to_delete_by_id = ['tweet_metadata.csv', 'twitter_user.csv', 'user_profile.csv']

    rest = [file for file in files if file not in to_delete_by_id]

    files = os.listdir(folder)
    for dele in to_delete_by_id:
        if dele in files and dele not in skip:
            print("Dropping %s" % dele)
            df = pd.read_csv(folder + dele, keep_default_na=False)#, lineterminator = '\n')
            df = df.sort_values('access', ascending=True).drop_duplicates('id', keep = 'last')
            df.to_csv(folder + dele, index=False)

    for dele in rest:
        if dele in files and dele not in skip:
            print("Dropping %s" % dele)
            df = pd.read_csv(folder + dele, keep_default_na=False)
            df = df.drop_duplicates(keep='last')

            if(len(df) == 0): # if empty
                os.remove(folder + dele)
            else:
                df.to_csv(folder + dele, index=False)


def main():
    input_folder = './output/'

    parser = argparse.ArgumentParser(
        description="Create views.", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("-i", "--input", dest="input_folder",
                        default=input_folder,
                        help="Name of the folder to read from, defaults to input")


    options = parser.parse_args()
    input_folder = "./" + options.input_folder + "/"

    return input_folder

def merge_reduce(main_folder = './output/'):
    '''
    Merge the tables into one and then drop duplicates
    :param main_folder: str
    :return:
    '''
    new_folder = 'merged_tables/'
    os.makedirs(main_folder + new_folder)

    folders = os.listdir(main_folder)

    counter = 0
    for i, folder in enumerate(folders):
        # if('created_tables_' in folder):
        counter += 1
        for file in os.listdir(main_folder + folder):
            with open(main_folder + folder + "/" + file, 'r') as f:
                if(counter > 1):
                    skippedHeader = False
                else:
                    skippedHeader = True
                with open(main_folder + new_folder + file, 'a') as g:
                    for line in f.readlines():
                        if(skippedHeader == False): # to skip headers after the first file
                            skippedHeader = True
                        else:
                            g.write(line)

        if(counter > 1):
            print("Dropping duplicates after %s" % (folder))
            drop_duplicates(main_folder + new_folder)

if __name__ == '__main__':
    output = main()
    merge_reduce(output)