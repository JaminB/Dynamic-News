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
        self.articles = []
        self.dateString = dateString
        self.feedzillaBlob = StoredQueries().get_feedzilla_by_date(dateString)
    
    def print_json_response(self):
        import json

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
            self.articles.append({'id': id, 'title': title, 'source': source, 'source_url': source_url, 'summary': summary, 'publish_date': str(publish_date), 'location': location, 'tags': tags})

            print('{'+json.dumps(self.articles, sort_keys=True, indent=4, separators=(',', ': '))+'}')


        


class Twitter:
    def __init__(self, newsid):
        self.tweets = []
        self.newsid = newsid
        self.twitterBlob = StoredQueries().get_tweet_by_id(newsid)

    def print_json_reponse(self):
        import json
        for i in range(0, len(self.twitterBlob)):
            tweet_id = self.twitterBlob[i][0]
            news_id = self.twitterBlob[i][1]
            screen_name = self.twitterBlob[i][2]
            created_at = str(self.twitterBlob[i][3])
            hashtags = self.twitterBlob[i][4][:-1].replace("b'","").replace("'", '').split('|')
            location = self.twitterBlob[i][5]
            coordinates = self.twitterBlob[i][6]
            text = self.twitterBlob[i][7].replace("b'","'").replace('b','')
            follower_count = self.twitterBlob[i][8]
            self.tweets.append({'tweet_id': tweet_id, 'news_id': news_id, 'screen_name': screen_name, 'created_at': created_at, 'hashtags': hashtags, 'location': location, 'coordinates': coordinates, 'text': text, 'follower_count': follower_count})
            print('{'+json.dumps(self.tweets, sort_keys=True, indent=4, separators=(',', ': '))+'}')


#Feedzilla('2015-02-13 13:56:00').print_json_response()

Twitter('43').print_json_reponse()


