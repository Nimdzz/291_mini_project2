# !/usr/bin/env python3

import re
import json
import string
import time

import pymongo

DBNAME = "291db"
POSTSPATH = "Posts/Posts.json"
TAGSPATH = "Posts/Tags.json"
VOTESPATH = "Posts/Votes.json"
P = "Posts"
V = "Votes"
T = "Tags"


def get_mongo_cli(mongo_port=27017):
    return pymongo.MongoClient('localhost', mongo_port)


def document_store(mongo_port: int = 27017):
    start = time.time()
    client = get_mongo_cli(mongo_port)
    print("connected mongodb at localhost, port:", mongo_port)
    db = client[DBNAME]
    print("created database", DBNAME)
    db.drop_collection(P)
    db.drop_collection(T)
    db.drop_collection(V)
    print("removed collection if exists")
    with open(POSTSPATH) as f:
        posts = json.load(f)
    res = db[P].insert_many(posts["posts"]["row"])
    print("inserted", len(res.inserted_ids), " posts")

    with open(TAGSPATH) as f:
        tags = json.load(f)
    res = db[T].insert_many(tags["tags"]["row"])
    print("inserted", len(res.inserted_ids), " tags")

    with open(VOTESPATH) as f:
        votes = json.load(f)
    res = db[V].insert_many(votes["votes"]["row"])
    print("inserted", len(res.inserted_ids), " votes")
    print("buid document store finished. used",
          round(time.time()-start, 2), "seconds\n")
    
    print("now extracting terms and creating index")
    db[P].create_index([("Id", pymongo.ASCENDING)], background=True)
    for doc in db[P].find():
        doc_id = doc["Id"]
        texts = []
        if "Title" in doc:
            texts.append(doc["Title"])
        if "Body" in doc:
            texts.append(doc["Body"])
        db["Posts"].update(
            {"Id": doc_id}, {"$set": {"terms": text2term(" ".join(texts).strip())}})
    print("extracted terms")
    db[P].create_index([("terms", pymongo.ASCENDING)], background=True)
    db[P].create_index([("Tags", pymongo.ASCENDING)], background=True)
    db[T].create_index([("TagName", pymongo.ASCENDING)], background=True)
    print("created index on posts terms")


def text2term(texts: str) -> list:
    """ extract terms from text
    """
    texts = re.sub("<.+?>", " ", texts)  # remove html tag
    texts = texts.lower()  # lower case
    for p in string.punctuation:
        texts = re.sub(re.escape(p), " ", texts)
    terms = re.split(r"\s+", texts)
    terms = list(set(terms))  # remove duplicated
    # avoid key too long error
    return [t for t in terms if 30 > len(t) >= 3 and t.isalnum()]


if __name__ == "__main__":
    port_num = input("please input mongodb server port(default is 27017):")
    if port_num.isnumeric():
        print("use input port:", port_num)
        document_store(port_num)
    else:
        print("use default port 27017")
        document_store()
