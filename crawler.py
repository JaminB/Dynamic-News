#!/usr/bin/python3
__author__ = 'Jamin Becker'

def feedzilla_date_convert(dateString):
    """feedzilla date conversion method"""
    months = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    splitDate = str(dateString).split(' ')
    day = splitDate[1]
    month = months[splitDate[2]]
    year = splitDate[3]
    time = splitDate[4]
    #print(year + '-' + month + '-' + day + ' ' + time)
    return year + '-' + month + '-' + day + ' ' + time

def twitter_date_convert(dateString):
    #print(dateString)
    months = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08', 'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    splitDate = str(dateString).split(' ')
    day = splitDate[2]
    month = months[splitDate[1]]
    year = splitDate[len(splitDate) - 1]
    time = splitDate[len(splitDate) - 3]
    #print(year + '-' + month + '-' + day + ' ' + time)
    return year + '-' + month + '-' + day + ' ' + time

def get_current_date_time():
    from datetime import datetime
    now = datetime.now()
    return str(now)[0:str(now).index('.')]

def get_current_date_time_minus(min):
    from datetime import datetime, timedelta
    now = datetime.now() - timedelta(minutes=min)
    return str(now)[0: str(now).index('.')]

def get_date_time_minus(date, min):
    from datetime import datetime, timedelta
    format = '%Y-%m-%d %H:%M:%S'
    date = datetime.strptime(date, format)
    return date - timedelta(minutes=min)

def first_date_larger(firstDate, secondDate):
    from datetime import datetime
    format = '%Y-%m-%d %H:%M:%S'
    firstDate = datetime.strptime(firstDate, format)
    secondDate =  datetime.strptime(secondDate, format)
    return firstDate > secondDate


class HTTP:
    """Simple Accessor for HTTP data; pulls down webpages"""

    def __init__(self, url):
        from urllib import request
        self.url = url
        print('--Downloading Data from - ' + self.url + '--')
        response = request.urlopen(url)
        self.html = response.read().decode('ascii', 'ignore')

    def get_html(self):
        print('--Finished--')
        return self.html


class MySql:
    """Super Tiny database connector requires pymysql to be installed
        use 'pip3 install pymysql' to install"""
    def __init__(self, query):
        import pymysql, config_parser
        self.query = []
        self.settings = config_parser.DatabaseConfig().get_config()
        conn = pymysql.connect(
            host=self.settings['database_ip'],
            port=int(self.settings['database_port']),
            user=self.settings['database_user'],
            passwd=self.settings['database_password'],
            db=self.settings['database_name'])
        cur = conn.cursor()
        cur.execute(query)
        for row in cur:
            if row != None:
                self.query.append(row)
        cur.close()
        conn.commit()
        conn.close()

    def get_result(self):
        return self.query

class StoredQueries:
    def __init__(self):
        import config_parser
        self.queries = config_parser.DatabaseQueries().get_queries()

    def get_last_id(self):
        return self.queries['last_feedzilla_id']

    def insert_news_article(self, title, source, source_url, summary, publish_date, url, location, tags, latitude='', longitude=''):
        if MySql(self.get_last_id()).get_result()[0][0] == None:
            id = '0'
        else:
            id = str(int(MySql(self.get_last_id()).get_result()[0][0]) + 1) #gets the last primary key id in the database, and adds one

        return str(MySql(self.queries['insert_news_article']\
            .replace('[id]',id)\
            .replace('[title]',title)\
            .replace('[source]',source)\
            .replace('[source_url]',source_url)\
            .replace('[summary]',summary)\
            .replace('[publish_date]',publish_date)\
            .replace('[url]',url)\
            .replace('[location]', location)\
            .replace('[tags]', tags)\
            .replace('[latitude]', str(latitude))\
            .replace('[longitude]', str(longitude))).get_result())

    def insert_tweet(self, twitter_id,  id, screen_name, created_at, hashtags, location, coordinates, text, follower_count):
        return str(MySql(self.queries['insert_tweet']\
            .replace('[twitter_id]', twitter_id)\
            .replace('[news_id]', id)\
            .replace('[screen_name]', screen_name)\
            .replace('[created_at]', created_at)\
            .replace('[hashtags]', hashtags)\
            .replace('[location]', location)\
            .replace('[coordinates]', coordinates)\
            .replace('[text]', text)\
            .replace('[follower_count]', follower_count)).get_result())

    def get_relevant_article_tags(self, mindatetime):
        return MySql(str(self.queries['get_relevant_article_tags']).replace('[date]', mindatetime)).get_result()


    def is_continent(self, continent):
        if len(MySql(str(self.queries['get_news_continent']).replace('[continent]', continent).replace('[equals]', '=')).get_result()) != 0:
            return True
        return False

    def is_country(self, country):
        if len(MySql(str(self.queries['get_news_country']).replace('[country]', country).replace('[equals]', '=')).get_result()) != 0:
            return True
        return False

    def is_a_an_the(self, article):
        if len(MySql(str(self.queries['get_article_adj']).replace('[article]', article).replace('[equals]', '=')).get_result()) != 0:
            return True
        return False

    def is_pronoun(self, pronoun):
        if len(MySql(str(self.queries['get_pronoun']).replace('[pronoun]', pronoun).replace('[equals]', '=')).get_result()) != 0:
            return True
        return False

    def is_preposition(self, verb):
        if len(MySql(str(self.queries['get_preposition']).replace('[preposition]', verb).replace('[equals]', '=')).get_result()) != 0:
            return True
        return False

    def is_verb(self, verb):
        if len(MySql(str(self.queries['get_verb']).replace('[verb]', verb).replace('[equals]', '=')).get_result()) != 0:
            return True
        return False

    def is_conjunction(self, conjunction):
        if len(MySql(str(self.queries['get_conjunction']).replace('[conjunction]', conjunction).replace('[equals]', '=')).get_result()) != 0:
            return True
        return False

    def does_article_have_coordinates(self, location):
        result = MySql(str(self.queries['get_feedzilla_latitude_by_location']).replace('[equals]','=').replace('[location]',location)).get_result()
        if result == []:
            return False
        return True

    def does_article_exist(self, title):
        try:
            MySql(str(self.queries['get_feedzilla_id_by_title']).replace('[equals]','=').replace('[title]',title)).get_result()[0][0]
        except IndexError:
            return False
        return True



class JSON:
    """Decode and encode raw JSON"""

    def __init__(self, jsonstr):
        import json
        self.json = jsonstr
        self.jsonObject = json.loads(jsonstr)

    def get_json_object(self):
        return self.jsonObject



class NewsArticle:
    """Object to store news articles in"""
    def __init__(self, publish_date, source, source_url, summary, title, url):
        from pymysql import escape_string
        import config_parser
        self.settings = config_parser.CrawlerConfig().get_config()
        self.publishDate = publish_date
        self.source = source
        self.sourceURL = source_url
        self.summary = summary
        self.title = title
        self.url = url
        self.location = ''
        self.tags = ''
        self.latitude = '-1'
        self.longitude = '-1'
        if not StoredQueries().does_article_exist(escape_string(self.title)):
            self._analyze_summary()
            self._derive_coordinates()

    def _rank_word(self, word):
        score = 0
        if len(word) > 0:
            if str(word)[0].isupper():
                score += 5
            for letter in word:
                score += 1
        return score


    def _analyze_summary(self):
        import re, operator
        print('\nAnalyzing Article Summary and extracting tags...')
        words = []
        cities = []
        countries = []
        continents = []
        tags = []
        selectTags = []
        tokenizedSummary = str(self.summary).split(' ')
        queries = StoredQueries()
        for textBlock in tokenizedSummary:
            miniTextBlock = re.sub(r'\W+', ' ', textBlock).split(' ')
            for word in miniTextBlock:
                words.append(word)
        for word in words:
            if not queries.is_a_an_the(word):
                if not queries.is_pronoun(word):
                    if not queries.is_preposition(word):
                        if not queries.is_conjunction(word):
                            if not queries.is_continent(word):
                                if not queries.is_country(word):
                                        tags.append((word, self._rank_word(word)))
                                else:
                                    countries.append(word)
                            else:
                                continents.append(word)

        tags.sort(key=operator.itemgetter(1), reverse=True)
        for tag in tags:
            if tag[0] not in selectTags:
                if len(selectTags) < 2:
                    selectTags.append(tag[0])
        for tag in selectTags:
            self.tags += tag + '|'

        if len(continents) != 0:
            self.location = continents[0]
        if len(countries) != 0:
            self.location = countries[0]

    def _derive_coordinates(self):
        geo_api_url = self.settings['geo_api_url']
        geo_api_key = self.settings['geo_api_key']
        print('Deriving coordinates from location: ' + self.get_location(), end='')
        try:
            coordinates = JSON(HTTP(geo_api_url.replace('[location]', self.get_location()).replace('[equals]', '=').replace('[key]', geo_api_key)).get_html()).get_json_object()['results'][0]['geometry']['location']
        except:
            print,('...Failure')
        try:
            self.latitude = coordinates['lat']
            self.longitude = coordinates['lng']
            print('...Success')
        except:
            return


    def get_publish_date(self):
        return self.publishDate

    def get_source(self):
        return self.source

    def get_source_url(self):
        return self.sourceURL

    def get_summary(self):
        return self.summary

    def get_title(self):
        return self.title

    def get_url(self):
        return self.url

    def get_location(self):
        return self.location

    def get_tags(self):
        return self.tags

    def get_latitude(self):
        return self.latitude

    def get_longitude(self):
        return self.longitude



class NewsGrabber:
    """Provides a realtime list of news articles"""

    def __init__(self):
        import config_parser
        self.settings = config_parser.CrawlerConfig().get_config()
        self.rawData = HTTP(self.settings['world_news_url'].replace('[equals]', '=')).html
        self.articles = JSON(self.rawData).jsonObject['articles']
        self.cachedArticles = self._cache_articles()

    def _cache_articles(self):
        cachedArticles = []
        for article in self.articles:
            cachedArticles.append(NewsArticle(
                article['publish_date'],
                article['source'],
                article['source_url'],
                article['summary'],
                article['title'],
                article['url']))
        return cachedArticles

    def _get_world_news_url(self):
        return self.settings['world_news_url'].replace('[equals]','=')

    def _get_base_url(self):
        return self.settings['base_url']

    def get_world_news_articles_raw(self):
        return self.rawData

    def get_world_news_articles_json(self):
        return self.articles

    def store_articles(self):
        from pymysql import escape_string, err
        skipCount = 0
        for article in self.cachedArticles:
            try:
                StoredQueries().insert_news_article(escape_string(article.get_title()),
                                           escape_string(article.get_source()),
                                           article.get_source_url(),
                                           escape_string(article.get_summary().replace('\n',' ')),
                                           feedzilla_date_convert(article.get_publish_date()),
                                           article.get_url(),
                                           article.get_location(),
                                           article.get_tags(),
                                           article.get_latitude(),
                                           article.get_longitude()
                                           )
                print("ADDING NEWS ARTICLE: " + article.get_publish_date() + ', ' + article.get_title()[0:30] + ', ' + article.get_location() + "")
            except err.IntegrityError:
                skipCount+=1
                #print("DUPLICATE NEWS ARTICLE: '" + article.get_publish_date() + ', ' + article.get_title()[0:30] + ", " + article.get_location())
        print(str(skipCount) + " articles skipped as they already exist in the database.")
class Tweet():
    def __init__(self, twitter_id, news_id, screen_name, created_at, hashtags, location, coordinates, text, followers_count):
        self.twitter_id = twitter_id
        self.news_id = news_id
        self.screen_name = screen_name
        self.created_at = created_at
        self.hashtags = hashtags
        self.location = location
        self.coordinates = coordinates
        self.text = text
        self.followers_count = followers_count

    def get_twitter_id(self):
        return self.twitter_id

    def get_news_id(self):
        return self.news_id

    def get_screen_name(self):
        return self.screen_name

    def get_created_date(self):
        return self.created_at

    def get_hashtags(self):
        return self.hashtags

    def get_location(self):
        return self.location

    def get_coordinates(self):
        return self.coordinates

    def get_text(self):
        return self.text

    def get_follower_count(self):
        return self.followers_count


class TweetGrabber():
    def __init__(self, keywords, newsid, publishDate):
        import config_parser
        from TwitterSearch import TwitterSearchOrder
        from TwitterSearch import TwitterSearch
        self.newsid = newsid
        self.publishDate = publishDate
        self.settings = config_parser.CrawlerConfig().get_config()
        self.keywords = keywords
        self.tweets = []
        self.tso = TwitterSearchOrder()
        self.tso.set_keywords(keywords)
        self.tso.set_language('en')
        self.tso.set_include_entities(True)
        self.ts = TwitterSearch(
        consumer_key = self.settings['consumer_key'],
        consumer_secret = self.settings['consumer_secret'],
        access_token = self.settings['access_token'],
        access_token_secret = self.settings['access_token_secret']
        )
        self.cachedTweets = self._cache_tweets()

    def _cache_tweets(self):
        tweets = []
        for tweet in self.ts.search_tweets_iterable(self.tso):
            created_at = str(twitter_date_convert(tweet['created_at']))
            publishDate = str(get_date_time_minus(self.publishDate, 360)) #6 hours prior to publishDate
            if first_date_larger(created_at, publishDate): #Make sure the tweet was posted after the article
                coordinates = None
                if tweet['coordinates'] != None:
                    coordinates = tweet['coordinates']['coordinates']
                tweets.append(Tweet(
                                    tweet['id'],
                                    self.newsid,
                                    tweet['user']['screen_name'],
                                    created_at,
                                    tweet['entities']['hashtags'],
                                    tweet['user']['location'],
                                    coordinates,
                                    tweet['text'],
                                    tweet['user']['followers_count']
                                    )
                )
        return tweets

    def store_tweets(self):
        from pymysql import escape_string, err
        skipCount = 0
        for tweet in self.cachedTweets:
            hashtags = ''
            for hashtag in tweet.get_hashtags():
                hashtags += str(hashtag['text'].encode('ascii', 'ignore')) + '|'
            try:
                StoredQueries().insert_tweet(
                                         str(tweet.get_twitter_id())[-5:],
                                         str(tweet.get_news_id()),
                                         str(tweet.get_screen_name()),
                                         str(tweet.get_created_date()),
                                         escape_string(str(hashtags)),
                                         escape_string(str(tweet.get_location().encode('ascii', 'ignore'))),
                                         str(tweet.get_coordinates()),
                                         escape_string(str(tweet.get_text().encode('ascii', 'ignore'))),
                                         str(tweet.get_follower_count()))
                print("ASSOCIATING TWEET TO NEWS ARTICLE(" + str(tweet.get_news_id()) + '): ' + str(tweet.get_created_date()[0:30]) + ', ' + str(tweet.get_twitter_id()) + ', ' + tweet.get_screen_name() + ', ' + tweet.get_location())
            except err.IntegrityError:
                    skipCount+=1
                    #print("DUPLICATE TWEET: " + str(tweet.get_created_date()) + ', ' + str(tweet.get_twitter_id()) + ', ' + tweet.get_location())


        print(str(skipCount) + " tweets skipped as they already exist in the database.")
def run():

    from time import sleep
    from TwitterSearch import TwitterSearchException
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-dd", "--days", help="Associate articles within <days>", type=int)
    parser.add_argument("-hh", "--hours", help="Associate articles within <hours>", type=int)
    parser.add_argument("-mm", "--minutes", help="Associate articles within <minutes>", type=int)
    args = parser.parse_args()
    if args.minutes == None:
        args.minutes = 0
    if args.days == None:
        args.days = 0
    if args.hours == None:
        args.hours = 0
    tolerence = (int(args.days) * 1440) + (int(args.hours) * 60) + int(args.minutes)
    while True:
        print('Storing News Articles...')
        print('Only associating articles to tweets where the article publish date is greater than: ' + str(get_date_time_minus(get_current_date_time(), tolerence)))
        NewsGrabber().store_articles()
        idsTagsDatesLocations = StoredQueries().get_relevant_article_tags(str(get_date_time_minus(get_current_date_time(), tolerence)))
        pauseIteration = False
        stepBack = 0
        for i in range(0, len(idsTagsDatesLocations)):
            if pauseIteration:
                i-=stepBack
            id = idsTagsDatesLocations[i][0]
            tags = str(idsTagsDatesLocations[i][1]).split('|')
            for j in range(0, len(tags)):
                if tags[j] == '':
                    tags.__delitem__(j)

            publishDate = str(idsTagsDatesLocations[i][2])
            location = str(idsTagsDatesLocations[i][3])
            tags.append(location)
            print('Searching Twitter for... article id:' + str(id) + ', using tags: ' + str(tags))
            try:
                TweetGrabber(tags, id, publishDate).store_tweets()
                pauseIteration = False
                stepBack = 0
            except TwitterSearchException:
                pauseIteration = True
                stepBack += 1
                print('Sleeping...Twitter API query quota reached.')
                sleep(5)
                print('Checking for new articles while we wait...')
                sleep(1)
                NewsGrabber().store_articles()
                sleep(10)
                print('Trying again...')
            print('Only associating articles to tweets where the article publish date is greater than: ' + str(get_date_time_minus(get_current_date_time(), tolerence)))
            idsTagsDatesLocations = StoredQueries().get_relevant_article_tags(str(get_date_time_minus(get_current_date_time(), tolerence)))



run()
