import sys
import re
from datetime import datetime
from typing import Counter
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import tweepy
from collections import defaultdict
from config import create_api
from database import create_db

# Show all columns when printing dataframe objects
pd.set_option('display.max_columns', None)

# Set seaborn style
sns.set_theme(font_scale=0.65, palette="pastel")

# Create DB Engine and API Object
con = create_db(log=False)
api = create_api()


def get_all_tweets(screen_name):
    """ Get all tweets by screen_name. Max tweets up to 3250, retricted by Twitter """
    # tweet id can be repeated due to self-retweet. Can not set primary key to retweet_id
    # Create a empty dict with default empty list as values
    tweets = defaultdict(list)

    # status = tweet
    for status in tweepy.Cursor(api.user_timeline, screen_name=screen_name, tweet_mode="extended", count=200).items():
        # When using extended mode with a Retweet,
        # the full_text attribute of the Status object may be truncated
        # with an ellipsis character instead of containing the full text of the Retweet
        # To get the full text of the Retweet, dive into 'retweeted_status'
        # Check if retweeted_status exists
        is_retweet = hasattr(status, 'retweeted_status')
        # change the status root to retweeted_status
        tweets['screen_name'].append(status.user.screen_name)
        tweets['created_at'].append(
            datetime.strftime(status.created_at, '%Y-%m-%d'))
        if is_retweet:
            status = status.retweeted_status
            tweets['retweet_screen_name'].append(status.user.screen_name)
            tweets['retweet_created_at'].append(
                datetime.strftime(status.created_at, '%Y-%m-%d'))
        else:
            tweets['retweet_screen_name'].append(None)
            tweets['retweet_created_at'].append(None)

        tweets['tweet_id'].append(status.id)
        tweets['body'].append(status.full_text)
        tweets['user_id'].append(status.user.id)
        tweets['favorite_count'].append(status.favorite_count)
        tweets['retweet_count'].append(status.retweet_count)

    # Set primary key columns as index
    df = pd.DataFrame(tweets).set_index(['created_at', 'tweet_id'])
    # A temporary table for deleting the existing rows from tweets table
    df.to_sql('tweets_tmp', con, index=True, if_exists='replace')

    try:
        # delete rows that we are going to update
        con.execute(
            'DELETE FROM tweets WHERE (created_at, tweet_id) IN (SELECT created_at, tweet_id FROM tweets_tmp)')
        con.commit()

        # insert and update table
        df.to_sql('tweets', con, index=True, if_exists='append')
    except Exception as e:
        print(e)
        con.rollback()

    # dump a json file to inspect the context
    # with open('test.json', 'w') as fh:
    #     json_obj = json.dumps(test[1]._json, indent=4, sort_keys=True)
    #     fh.write(json_obj)

    # Save to a csv file for debugging
    print(df[['body', 'favorite_count']])
    df.to_csv('data.csv')

# under construction
def get_users_profile(screen_name):
    """ Get user basic profiles by screen_name """
    users = defaultdict(list)
    user = api.get_user(screen_name=screen_name)
    users['user_id'].append(user.id)
    users['screen_name'].append(user.screen_name)
    users['name'].append(user.name)
    users['location'].append(user.location)
    users['description'].append(user.description)
    users['followers_count'].append(user.followers_count)
    users['friends_count'].append(user.friends_count)
    users['statuses_count'].append(user.statuses_count)

    # Set primary key column as index
    df = pd.DataFrame(users).set_index(['user_id'])
    print(df)
    # A temporary table for deleting the existing rows from tweets table
    df.to_sql('users_profile_tmp', con, index=True, if_exists='replace')

    try:
        # delete rows that we are going to update
        con.execute(
            'DELETE FROM users_profile WHERE user_id IN (SELECT user_id FROM users_profile_tmp)')
        con.commit()

        # insert and update table
        df.to_sql('users_profile', con, index=True, if_exists='append')
    except Exception as e:
        print(e)
        con.rollback()

    # with open('record.json', 'w') as fhandler:
    #     json.dump(user._json, fhandler)

    # import pprint
    # import inspect
    # inspect the method of 'user' object
    # pprint.pprint(inspect.getmembers(user, predicate=inspect.ismethod))
    # show only 20 followers
    # for follower in user.followers():
    #     print(follower.name)


# Read the predefined keywords from ./keywords.txt line by line and store into a list.
keywords = []
with open('keywords.txt', 'r', encoding="utf-8") as fh:
    keywords = [line.strip().lower() for line in fh]

# convert a list into a single sql command for filtering the keywords
sql_keywords = ' OR '.join(
    [f'body LIKE \'%{kw.strip().lower()}%\'' for kw in keywords])


# a helper function that extracts all the keywords from a tweet and store into a string
def _get_keywords(row):
    matched_keywords = []
    for kw in keywords:
        if kw in row.lower():
            matched_keywords.append(kw)
    return ','.join(matched_keywords)


def read_data(screen_name):
    """Read data(body) based on keywords"""

    sql = \
        f"""
        SELECT created_at, body, favorite_count, retweet_count FROM tweets
            WHERE UPPER(screen_name)=UPPER('{screen_name}')
            AND ({sql_keywords})
        """
    # sql2 = f"SELECT * FROM tweets WHERE UPPER(screen_name)=UPPER('{screen_name}')"

    try:
        cur = con.cursor()
        cur.execute(sql)
        result = cur.fetchall()
        
        df = pd.DataFrame(result, columns=['Date', 'Result', 'favorite_count', 'retweet_count'])
        df['Date'] = df['Date'].astype('datetime64')
        df.set_index('Date', inplace=True)
        df['Keywords'] = df['Result'].apply(lambda row: _get_keywords(row))
        df.to_csv("READ.csv", index=True)
        print(df)

        # count the keywords
        cnt = Counter()
        for kws in df.Keywords:
            for kw in kws.split(','):
                cnt[kw] += 1

        # Convert counter to dataframe
        pd_cnt = pd.DataFrame.from_dict(cnt, orient='index').reset_index().rename(columns={'index':'Keywords', 0:'Count'})
        pd_cnt_sorted_desc = pd_cnt.sort_values(by=['Count'], ascending=False).reset_index(drop=True)
        print(pd_cnt_sorted_desc)

        # Visualize the occurance of the keywords
        _ = sns.barplot(x='Count', y='Keywords', data=pd_cnt_sorted_desc)
        _.set(title='Occurrance of Keywords')
        _ = sns.jointplot(x='favorite_count', y='retweet_count', data=df.reset_index(drop=True))
        plt.show()

    except Exception as e:
        print(e)


def get_followers(screen_name):
    """ Get followers by screen_name """
    followers = defaultdict(list)

    for follower in tweepy.Cursor(api.get_followers, screen_name=screen_name, count=200).items():
        followers['follower_screen_name'].append(screen_name)
        followers['user_id'].append(follower.id)
        followers['screen_name'].append(follower.screen_name)
        followers['name'].append(follower.name)
        followers['location'].append(follower.location)
        followers['description'].append(follower.description)
        followers['followers_count'].append(follower.followers_count)
        followers['friends_count'].append(follower.friends_count)
        followers['statuses_count'].append(follower.statuses_count)

        if len(followers['user_id']) > 1000:
            break;
    # Set primary key column as index
    df = pd.DataFrame(followers).set_index(['follower_screen_name', 'user_id'])
    df.to_csv('test.csv')
    print(df)
    # A temporary table for deleting the existing rows from followers table
    df.to_sql('followers_tmp', con, index=True, if_exists='replace')

    try:
        # delete rows that we are going to update
        con.execute(
            """DELETE FROM followers
                    WHERE (follower_screen_name, user_id)
                        IN (SELECT follower_screen_name, user_id FROM followers_tmp)""")
        con.commit()

        # insert and update table
        df.to_sql('followers', con, index=True, if_exists='append')
    except Exception as e:
        print(e)
        con.rollback()

def get_friends(screen_name):
    """ Get friends by screen_name """
    friends = defaultdict(list)

    for friend in tweepy.Cursor(api.get_friends, screen_name=screen_name, count=200).items():
        friends['following_screen_name'].append(screen_name)
        friends['user_id'].append(friend.id)
        friends['screen_name'].append(friend.screen_name)
        friends['name'].append(friend.name)
        friends['location'].append(friend.location)
        friends['description'].append(friend.description)
        friends['followers_count'].append(friend.followers_count)
        friends['friends_count'].append(friend.friends_count)
        friends['statuses_count'].append(friend.statuses_count)

        if len(friends['user_id']) > 1000:
            break;
    # Set primary key column as index
    df = pd.DataFrame(friends).set_index(['following_screen_name', 'user_id'])
    df.to_csv('test.csv')
    print(df)
    # A temporary table for deleting the existing rows from friends table
    df.to_sql('friends_tmp', con, index=True, if_exists='replace')

    try:
        # delete rows that we are going to update
        con.execute(
            """DELETE FROM friends
                    WHERE (following_screen_name, user_id)
                        IN (SELECT following_screen_name, user_id FROM friends_tmp)""")
        con.commit()

        # insert and update table
        df.to_sql('friends', con, index=True, if_exists='append')
    except Exception as e:
        print(e)
        con.rollback()
    

if __name__ == '__main__':
    # implement a simple command line interface
    if len(sys.argv) == 3:
        args_str = ' '.join(sys.argv[1:])
        # Regex for matching the pattern : app.py [-utfra] [screen_name]
        r = re.compile('^-(?P<options>[utfra]+)\s+(?P<arg>\w+)$')
        m = r.match(args_str)
        # If there are matches
        if m is not None:
            args_dict = m.groupdict()
            # Get basic user profile
            # usage: python app.py -u [screen_name]
            if 'u' in args_dict['options']:
                get_users_profile(args_dict['arg'])

            # Get all tweets up to 3250
            # usage: python app.py -t [screen_name]
            if 't' in args_dict['options']:
                get_all_tweets(args_dict['arg'])

            # Get 1000 latest followers
            # usage: python app.py -f [screen_name]
            if 'f' in args_dict['options']:
                get_followers(args_dict['arg'])

            # Get 1000 latest friends
            # usage: python app.py -r [screen_name]
            if 'r' in args_dict['options']:
                get_friends(args_dict['arg'])

            # Read tweets and make simple analysis
            # usage: python app.py -a [screen_name]
            if 'a' in args_dict['options']:
                read_data(args_dict['arg'])
        else:
            print("""
                        Incorrect usage:
                        app.py [-utfra] [screen_name]
                  """)



con.close() 