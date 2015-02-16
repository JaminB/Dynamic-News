__author__ = 'Jamin Becker'

class Generic(object):
    """Generic Parser for config files, keywords are passed to this class during runtime. Results are stored in 'values'
        Uses dictionary datastructure to avoid shitty metaprogramming techniques"""


    def __init__(self, configFile, keywordList, ignoreList):
        self.keywords = keywordList
        self.ignoreList = ignoreList
        self.configFile = configFile
        self.values = {}
        self._digest()

    def _digest(self):
        f = open(self.configFile, "r")
        content = f.readlines()
        lineNum = 0
        for line in content:
            lineNum += 1
            if "=" in line:
                variable = line.split("=")[0].strip()
                if variable in self.keywords:
                    if len(line.split("=")) == 2:
                        self.values[variable] = line.split("=")[1].strip().replace('"','')
                    else:
                        print ("Error on line: [" + str(lineNum)+ "] variable[" + variable + "] has no assignment after '='")
                        return
                else:
                    if variable not in self.ignoreList:
                        print ("Error on line: [" + str(lineNum)+ "] variable[" + variable + "] is not valid")
                        return
            else:
                print ("Error on line: [" + str(lineNum)+ "] no '=' between variable and value")
                return

    def __str__(self):
        keyValPairs = ""
        for value in self.values:
            keyValPairs += (value + " -> " + self.values[value] + "\n")
        return keyValPairs

class CrawlerConfig:
    def __init__(self):
        keywords = ['base_url', 'world_news_url', 'consumer_key', 'consumer_secret', 'access_token', 'access_token_secret' ]
        ignoreWords = ['database_ip', 'database_port',  'database_name', 'database_user', 'database_password']
        self.data = Generic('crawler.conf', keywords, ignoreWords)


    def get_config(self):
        return self.data.values

class DatabaseConfig:
    def __init__(self):
        keywords = ['database_ip', 'database_port',  'database_name', 'database_user', 'database_password']
        ignoreList = ['base_url', 'world_news_url', 'consumer_key', 'consumer_secret', 'access_token', 'access_token_secret']
        self.data = Generic('crawler.conf', keywords, ignoreList)

    def get_config(self):
        return self.data.values

class DatabaseQueries:
    def __init__(self):
        keywords = ['last_feedzilla_id','databases', 'insert_news_article', 'get_news_continent', 'get_news_country', 'get_relevant_article_tags', 'get_verb', 'get_preposition', 'get_pronoun', 'get_article_adj', 'get_conjunction', 'insert_tweet']
        ignoreList = []
        self.data = Generic('queries.conf', keywords, ignoreList)

    def get_queries(self):
        return self.data.values
