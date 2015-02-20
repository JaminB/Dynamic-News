__author__ = 'jamin'

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

class HTTP:
    """Simple Accessor for HTTP data; pulls down webpages"""

    def __init__(self, url):
        from urllib import request
        self.url = url
        response = request.urlopen(url)
        self.html = response.read().decode('ascii', 'ignore')

    def get_html(self):
        return self.html

class JSON:
    """Decode and encode raw JSON"""

    def __init__(self, jsonstr):
        import json
        self.json = jsonstr
        self.jsonObject = json.loads(jsonstr)

    def get_json_object(self):
        return self.jsonObject

class StoredQueries:
    def __init__(self):
        import config_parser
        self.queries = config_parser.DatabaseQueries().get_queries()

    def get_feedzilla_by_date(self, dateString):
        return MySql(str(self.queries['get_feedzilla_by_publish_date']).replace('[date]', dateString)).get_result()

    def get_tweet_by_id(self, newsid):
        return MySql(str(self.queries['get_twitter_by_id']).replace('[id]', newsid).replace('[equals]', '=')).get_result()

class Feedzilla:
    def __init__(self, dateString):
        from geopy import geocoders
        import config_parser
        self.settings = config_parser.CrawlerConfig().get_config()
        self.articles = []
        self.dateString = dateString
        self.feedzillaBlob = StoredQueries().get_feedzilla_by_date(dateString)

        for i in range(0, len(self.feedzillaBlob)):
            id = self.feedzillaBlob[i][0]
            title = self.feedzillaBlob[i][1]
            source = self.feedzillaBlob[i][2]
            source_url = self.feedzillaBlob[i][3]
            summary = self.feedzillaBlob[i][4]
            publish_date = self.feedzillaBlob[i][5]
            url = self.feedzillaBlob[i][6]
            location = self.feedzillaBlob[i][7]
            tags = self.feedzillaBlob[i][8][:-1].split('|')
            coordinates = [self.feedzillaBlob[i][9], self.feedzillaBlob[i][10]]
            self.articles.append({'id': id, 'title': title, 'source': source, 'source_url': source_url, 'summary': summary, 'publish_date': str(publish_date), 'location': location, 'coordinates': coordinates, 'tags': tags})

    def print_json_response(self):
        import json
        print('{'+json.dumps(self.articles, sort_keys=True, indent=4, separators=(',', ': '))+'}')

class Twitter:
    def __init__(self, newsid):
        self.tweets = []
        self.newsid = newsid
        self.twitterBlob = StoredQueries().get_tweet_by_id(newsid)
        for i in range(0, len(self.twitterBlob)):
            tweet_id = self.twitterBlob[i][0]
            news_id = self.twitterBlob[i][1]
            screen_name = self.twitterBlob[i][2]
            created_at = str(self.twitterBlob[i][3])
            hashtags = self.twitterBlob[i][4][:-1].replace("b'","").replace("'", '').split('|')
            location = self.twitterBlob[i][5].replace("b'",'').replace("'",'')
            coordinates = self.twitterBlob[i][6]
            text = self.twitterBlob[i][7].replace("b'","'").replace('b','')
            follower_count = self.twitterBlob[i][8]
            self.tweets.append({'tweet_id': tweet_id, 'news_id': news_id, 'screen_name': screen_name, 'created_at': created_at, 'hashtags': hashtags, 'location': location, 'coordinates': coordinates, 'text': text, 'follower_count': follower_count})

    def print_json_response(self):
        import json
        print('{'+json.dumps(self.tweets, sort_keys=True, indent=4, separators=(',', ': '))+'}')


class Correlate:
    def __init__(self, dateString):
        import math
        self.articles = Feedzilla(dateString).articles
        self.data = []
        locationList = []
        tweetSum = 0
        for i in range(0, len(self.articles)):
            id = self.articles[i]['id']
            tweets = len(Twitter(str(id)).tweets)
            tweetSum += tweets

        for j in range(0, len(self.articles)):
            latitude = self.articles[j]['coordinates'][0]
            longitude = self.articles[j]['coordinates'][1]
            id = self.articles[j]['id']
            tweets = len(Twitter(str(id)).tweets)
            tweetSum += tweets
            if tweetSum > 0:
                magnitude = tweets/tweetSum
            else:
                magnitude = tweets
            locationData = [round(float(latitude), 2), round(float(longitude), 2), magnitude]
            for element in locationData:
                locationList.append(element)
        allSeries = ['stories', locationList]
        self.data.append(allSeries)

    def print_json_response(self):
        import json
        print(json.dumps(self.data, sort_keys=True, indent=4, separators=(',', ': ')))


