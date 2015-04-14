__author__ = 'jamin'
import interfaces
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-dd", "--days", help="Associate articles within <days>", type=int)
parser.add_argument("-hh", "--hours", help="Associate articles within <hours>", type=int)
parser.add_argument("-mm", "--minutes", help="Associate articles within <minutes>", type=int)
parser.add_argument("-q", "--query", help="Options = [associated_tweets],[associated_articles],[random_tweet]", type=str)
parser.add_argument("-s", "--sort", help="Sort associated articles?", type=bool)
parser.add_argument("-t", "--top", help="Top 'n' associated articles", type=int)
parser.add_argument("-o", "--output_file", help="Output file name", type=str)
args = parser.parse_args()
if args.minutes == None:
    args.minutes = 0
if args.days == None:
    args.days = 0
if args.hours == None:
    args.hours = 0
if args.sort == None:
    args.sort == False
if args.top == None:
    args.top = 0
try:
    f = open(args.output_file, "w")
except TypeError:
    f = open("output.json", "w")
contentType = "Content-type:text/json\r\n\r\n"

def write_associated_tweets_by_time():
    f.write(contentType + interfaces.Correlate(days=args.days, hours=args.hours, minutes=args.minutes).get_json_response())
    f.close()

def write_associated_articles_by_time():
    f.write(contentType + interfaces.Correlate(days=args.days, hours=args.hours, minutes=args.minutes, sort_articles=args.sort).get_json_response(dataoutput=2, top=args.top))
    f.close()

def write_random_tweet():
    f.write(contentType + interfaces.Correlate(days=args.days, hours=args.hours, minutes=args.minutes).get_json_response(dataoutput=3))
    f.close()

if str(args.query).lower() == "associated_tweets":
    write_associated_tweets_by_time()
elif str(args.query).lower() == "associated_articles":
    write_associated_articles_by_time()
elif str(args.query).lower() == "random_tweet":
    write_random_tweet()
else:
    parser.print_help()