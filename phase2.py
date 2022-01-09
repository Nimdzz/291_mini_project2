# !/user/bin/env python3
import re
import json
import string
import time
import datetime
import traceback

import pymongo
# from bson.objectid import ObjectId

DBNAME = "291db"
P = "Posts"
V = "Votes"
T = "Tags"
PAGENUM = 5  # search result returned every page


class FakeStackOverflow(object):
    def __init__(self, host="localhost", mongo_port=27017, user_id=None):
        try:
            self.user_id = user_id
            self.mongo_client = pymongo.MongoClient(host, mongo_port)
            self.db = self.mongo_client[DBNAME]
            self.max_post_id = self.get_max_post_id()
            self.max_tag_id = self.get_max_tag_id()
            self.connected = True
        except Exception as e:
            print("connect mongo error", e)
            traceback.print_exc()
            self.connected = False

    def set_user_id(self, user_id):
        if not user_id.strip():
            print("cannot use empty id")
        else:
            print("set current user id as:", user_id)
            self.user_id = user_id

    def start(self):
        if self.connected:
            self.get_user_report()
            self.menu()
        else:
            print("Error: faild start a initialized clinet")

    def menu(self):
        print(""" 
        #################################
        #                               #
        # Welcome to Fake StackOverflow #
        #                               #
        # [0] Exit Fake StackOverflow   #
        # [1] Post a question           #
        # [2] Search a question         #
        #                               #
        #################################
        """)
        command = input("please input command num:")
        if command == '0':
            print("exited, bye")
            return
        elif command == '1':
            title = input("please input your question:")
            body = input("please describe your question detailly:")
            tags = input("please input zero or more tags, split by space:")
            if not tags.strip():
                tags = []
            else:
                tags = re.split(r"\s+", tags.lower().strip())
            insert_res = self.post_question(
                title.strip(), body.strip(), tags)
            print("your question is posted:")
            print(self.db[P].find({"_id": insert_res.inserted_id})[0])
            print("back to menu ...")
            self.menu()
        elif command == '2':
            keywords = input(
                "please input one or more keywords, split by space:")
            if not keywords.strip():
                print("invalid keywords")
                self.menu()
                return
            else:
                keywords = re.split(r"\s+", keywords.lower().strip())
                rst = self.search_question(keywords)
                pagenum = 1
                all_resluts = []
                goto_menu = False
                no_results = True
                for idx, p in enumerate(rst):
                    no_results = False
                    all_resluts.append(p["Id"])
                    title = p["Title"]
                    score = p["Score"]
                    answer_count = p["AnswerCount"]
                    date = p["CreationDate"]
                    seq_num = "".join(["[", str(idx), "]"])
                    print(seq_num, title)
                    print(" "*len(seq_num), "creation date:", date,
                          "score", score, "answer count:", answer_count, "\n")
                    if pagenum % PAGENUM == 0:
                        print("page", int(pagenum/PAGENUM), "result showed.")
                        sub_command = input(
                            "view posts please input sequence num; next page input N; back to menu input M: ")
                        if sub_command.isnumeric():
                            goto_menu = self.view_post(
                                all_resluts[int(sub_command)])
                            if goto_menu:
                                break
                        elif sub_command.lower() == "n":
                            pagenum += 1
                            continue
                        elif sub_command.lower() == "m":
                            goto_menu = True
                            break
                        else:
                            print("invalid command, back to menu")
                            goto_menu = True
                            break
                    pagenum += 1
                if no_results:
                    print("no results for your keywords, back to menu")
                    self.menu()
                if goto_menu:
                    self.menu()
        else:
            print("invalid command num, your input should be one of 0, 1, 2")
            self.menu()

    def get_max_post_id(self):
        # when nums > 12M, this method will get error
        # ids = self.db[P].distinct("Id")
        # return max(map(int, ids))
        max_id = self.db[P].find({}).sort(
            [("_id", pymongo.DESCENDING)]).limit(1)[0]
        return int(max_id["Id"])

    def get_max_tag_id(self):
        max_id = self.db[T].find().sort([("_id", -1)]).limit(1)[0]
        return int(max_id["Id"])

    def get_user_report(self):
        """(1) the number of questions owned and the average score for those questions
        (2) the number of answers owned and the average score for those answers
        (3) the number of votes registered for the user
        """
        if self.user_id == None:
            print("hello, Anonymous user!")
            return
        question_owned = self.db[P].aggregate([
            {"$match": {"OwnerUserId": self.user_id, "PostTypeId": "1"}},
            {"$group": {"_id": None, "question_num": {
                "$sum": 1}, "avg_score": {"$avg": "$Score"}}}
        ])
        print("hello, user", self.user_id)
        for r in question_owned:
            print("You have", r["question_num"],
                  "questions,", "avg score is", r["avg_score"])
        answer_owned = self.db[P].aggregate([
            {"$match": {"OwnerUserId": self.user_id, "PostTypeId": "2"}},
            {"$group": {"_id": None, "answer_num": {
                "$sum": 1}, "avg_score": {"$avg": "$Score"}}}
        ])
        for r in answer_owned:
            print("You have", r["answer_num"], "answers,",
                  "avg score is", r["avg_score"])
        votes = self.db[V].count_documents({"UserId": self.user_id})
        print("You have got", votes, "votes.")

    def post_question(self, title, body, tags: list = []):
        self.max_post_id += 1
        post = {
            "Id": str(self.max_post_id),
            "PostTypeId": "1",
            "CreationDate": datetime.datetime.now().isoformat(),
            "Title": title,
            "Body": body,
            "Score": 0,
            "ViewCount": 0,
            "AnswerCount": 0,
            "CommentCount": 0,
            "FavoriteCount": 0,
            "ContentLicense": "CC BY-SA 2.5"
        }
        if len(tags) != 0:
            tagstr = "><".join(tags)
            tagstr = "<"+tagstr+">"
            post["Tags"] = tagstr
        for t in tags:
            if not self.db[T].find_one({"TagName": t}):  # insert new tag
                self.max_tag_id += 1
                self.db[T].insert_one(
                    {"Id": str(self.max_tag_id), "TagName": t, "Count": 1})
            else:  # increment tag count
                self.db[T].update({"TagName": t}, {"Count": {"$inc": 1}})
        if self.user_id:
            post["OwnerUserId"] = self.user_id
        return self.db[P].insert_one(post)

    def search_question(self, keywords):
        keywords = list(map(lambda x: re.compile(x), keywords))  # like match, use re.compile to mock with $in
        print(keywords)
        res = self.db[P].aggregate([
            {
                "$match": {
                    "$or": [{"PostTypeId": "1", "terms": {"$elemMatch": {"$in": keywords}}},
                            {"PostTypeId": "1", "Tags": {"$in": keywords}}]
                }
            },
            # only return docs which have the following fields:
            {"$project": {"Title": 1, "CreationDate": 1,
                          "Score": 1, "AnswerCount": 1, "Id": 1}}
        ])
        return res

    def view_post(self, post_id):
        self.db[P].update({"Id": post_id}, {"$inc": {"ViewCount": 1}})
        post = self.db[P].find_one({"Id": post_id}, {"_id": 0})
        res = json.dumps(post, ensure_ascii=False, indent=2)
        print(res)
        print("selected post showed.")
        command = input(
            "back menu input M; answer this question input A; others will back to search result; ctrl+c exit program: ")
        if command.lower() == "m":
            return True  # tell father function goto menu
        elif command.lower() == "a":
            # todo Write answer action here
            return
        else:
            print("back to search result")
            return


if __name__ == "__main__":
    port_num = input("please input mongodb server port(default is 27017):")
    so = None
    if port_num.isnumeric():
        print("use input port:", port_num)
        so = FakeStackOverflow(mongo_port=port_num)
    else:
        print("use default port 27017")
        so = FakeStackOverflow()
    if input("do you want to input user id(Y/N):").lower() == "y":
        so.set_user_id(input("please input user id:"))
    so.start()
