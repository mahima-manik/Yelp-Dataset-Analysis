from textblob import TextBlob
import json
import jsonlines
filename="/home/sruti/Desktop/dataset/auto_madison.json"
path1="/home/sruti/Desktop/dataset/auto_reviews/"
total_rev=0
total_pos=0
total_stars=0
num_businesses=0
f=open(filename, "r")
rate=[]
for rw in f:
    f1=open(path1+"rev"+json.loads(rw)["business_id"]+".json", "r")
    positive=0
    negative=0
    neutral=0
    total=0
    num_businesses=num_businesses+1
    total_stars=total_stars+json.loads(rw)["stars"]
    for rw1 in f1:
        total_rev=total_rev+1
        total=total+1
        b=json.loads(rw1)["text"]
        analysis = TextBlob(b)
        lang=analysis.detect_language()
        if lang==u'en':
            if analysis.sentiment.polarity > 0:
                positive=positive+1
                total_pos=total_pos+1
                print json.loads(rw)["business_id"], " positive\n" 
            elif analysis.sentiment.polarity == 0:
                neutral=neutral+1
                print json.loads(rw)["business_id"], " neutral\n"
            else:
                negative=negative+1
                print json.loads(rw)["business_id"], " negative\n"
    print positive, negative, neutral, total
    d={"business_id":json.loads(rw)["business_id"],"positive":positive, "negative": negative, "neutral": neutral, "total":total, "latitude":json.loads(rw)["latitude"], "longitude":json.loads(rw)["longitude"], "name":json.loads(rw)["name"], "stars":json.loads(rw)["stars"]}
    rate.append(d)
    
f.close()
for d in rate:
    avg_num_votes=float(total_rev)/float(num_businesses)
    avg_rating_positive=float(5*total_pos)/float(total_rev)
    avg_rating_stars=float(total_stars)/float(num_businesses)
    this_num_votes=float(d["total"])
    this_rating_positive=float(5*d["positive"])/float(d["total"])
    this_rating_stars=float(d["stars"])
    rating_positive=float(float(avg_num_votes*avg_rating_positive)+float(this_num_votes*this_rating_positive))/float(avg_num_votes+this_num_votes)
    rating_stars=float(float(avg_num_votes*avg_rating_stars)+float(this_num_votes*this_rating_stars))/float(avg_num_votes+this_num_votes)
    d["rating_positive"]=rating_positive
    d["rating_stars"]=rating_stars
    
sorted_rating_positive=sorted(rate, key=lambda k: k["rating_positive"], reverse=True)
i=1
for l in sorted_rating_positive:
    l["rank_positive"]=i
    i=i+1

sorted_rating_stars=sorted(sorted_rating_positive, key=lambda k: k["rating_stars"], reverse=True)
i=1
for l in sorted_rating_stars:
    l["rank_stars"]=i
    i=i+1

f2=jsonlines.open("/home/sruti/Desktop/dataset/auto_ranks.json", mode="w")
for d in sorted_rating_stars:
    f2.write(d)
f2.close()
