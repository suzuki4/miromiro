# -*- coding: utf-8 -*-
"""
Created on Sat Apr 28 07:44:37 2018

@author: suzuki
"""

import requests
import json
import os
import boto3
from boto3.dynamodb.conditions import Key
import logging
import numpy as np
import pandas as pd
import fitbit
import base64
import urllib
import datetime
import decimal


AWS_BOTO3_ACCESS_KEY = os.environ["AWS_BOTO3_ACCESS_KEY"]
AWS_BOTO3_SECRET_KEY = os.environ["AWS_BOTO3_SECRET_KEY"]
AWS_REGION = "ap-northeast-1"

LINE_URL_REPLY = 'https://api.line.me/v2/bot/message/reply'
LINE_URL_PUSH = 'https://api.line.me/v2/bot/message/push'
LINE_HEADERS = {
    'Authorization': 'Bearer ' + os.environ['LINE_CHANNEL_ACCESS_TOKEN'],
    'Content-type': 'application/json'
}

FITBIT_CLIENT_ID = os.environ["FITBIT_CLIENT_ID"]
FITBIT_CLIENT_SECRET = os.environ["FITBIT_CLIENT_SECRET"]
FITBIT_REDIRECT_URI = "https://0knbiipk1h.execute-api.ap-northeast-1.amazonaws.com:443/prd/v1"
FITBIT_SCOPES = "activity heartrate location nutrition profile settings sleep social weight"
FITBIT_AUTH_URL = "https://www.fitbit.com/oauth2/authorize?response_type=code&client_id={}&redirect_uri={}&scope={}&expires_in=3600000".format(FITBIT_CLIENT_ID,urllib.parse.quote(FITBIT_REDIRECT_URI),urllib.parse.quote(FITBIT_SCOPES))

DATETIME_NOW = datetime.datetime.now()
BASE_PERIOD = 100

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class Error:    
    ERRORS = {1:"Fitbit連携時に拒否以外のerror",
              2:"Fitbit連携時にエラーはないがcodeがNone",
              3:"Fitbit連携時にauth_requestの結果がNone",
             }    

    @classmethod
    def code(cls, code):
        return cls.ERRORS[code]

def log_error(error_code):
    logger.error("[ERROR_CODE_{0:04d}]:{}".format(error_code, Error.code(error_code)))

##############################

class DynamoDB:
    
    def __init__(self):
        self.con = None

    def __connect_if_not(self):        
        if not self.con:
            self.__connect()     
        
    def __connect(self):
        self.con = boto3.resource(
                     'dynamodb',
                     aws_access_key_id=AWS_BOTO3_ACCESS_KEY,
                     aws_secret_access_key=AWS_BOTO3_SECRET_KEY,
                     region_name=AWS_REGION)
            
    def get_m_user(self, line_mid):
        self.__connect_if_not()        
        table = self.con.Table("m_user")
        res = table.get_item(Key={"line_mid":line_mid})
        return res.get("Item")

    def put_user(self, item):
        self.__connect_if_not()        
        table = self.con.Table("m_user")
        item = convert_to_decimal(item)
        logger.info("[DYNAMO_PUT]:{}".format(item))
        table.put_item(Item=item)
            
    def batch_write(self, table_name, items):
        self.__connect_if_not()        
        table = self.con.Table(table_name)

        with table.batch_writer() as batch:
            for item in items:
                item = convert_to_decimal(item)
                batch.put_item(Item=item)
        logger.info("[DYNAMO_BATCH_WRITE]:{{tbl_name:{},items:{}}}".format(table_name,items))
        
    def query_by_datetime(self, line_mid, table_name, datetime_key, datetime_str):
        self.__connect_if_not()       
        table = self.con.Table(table_name)

        res = table.query(
            KeyConditionExpression=Key("line_mid").eq(line_mid) & Key(datetime_key).gte(datetime_str), 
        )
        items = res["Items"]
        logger.info("[DYNAMO_QUERY]:{{tbl:{},line_mid:{},{}:{},result_cnt:{}}}".format(table_name, line_mid, datetime_key, datetime_str, len(items)))

        return items
 

class ExFitbit(fitbit.Fitbit):
    
    TBL_ACTIVITIES = ["calories","caloriesBMR","steps","distance","minutesSedentary","minutesLightlyActive","minutesFairlyActive","minutesVeryActive","activityCalories"]
    
    def __init__(self, m_user):
        super().__init__(FITBIT_CLIENT_ID,
                         FITBIT_CLIENT_SECRET,
                         access_token=m_user["access_token"],
                         refresh_token=m_user["refresh_token"],
                         refresh_cb=self.refresh_cb)
        self.m_user = m_user
        
    def refresh_cb(self, token):
        logger.info("[FITBIT]:refresh token {}".format(self.m_user["line_mid"]))
        FitbitAuthController(self.m_user["line_mid"]).register(token)

    def get_tbl_sleep(self):        
        query_datetime = DATETIME_NOW.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=BASE_PERIOD)
        return dynamo.query_by_datetime(self.m_user["line_mid"], "tbl_sleep", "endTime", query_datetime.strftime("%Y-%m-%dT00:00:00.000"))

    def update_tbl_sleep(self):
    
        query_datetime = DATETIME_NOW.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=BASE_PERIOD)
        tbl_sleep = self.get_tbl_sleep()
        
        if len(tbl_sleep) == 0:
            sleeps = self.get_sleep_range(query_datetime.strftime("%Y-%m-%d"), DATETIME_NOW.strftime("%Y-%m-%d"))
        
        else:
            df_sleep = pd.DataFrame.from_dict(tbl_sleep)
            max_dateOfSleep = df_sleep["dateOfSleep"].max()
            sleeps = self.get_sleep_range(max_dateOfSleep, DATETIME_NOW.strftime("%Y-%m-%d"))
        
        items = []
        for sleep in sleeps["sleep"]:
            del sleep["minuteData"]
            sleep["line_mid"] = self.m_user["line_mid"]
            if sleep not in tbl_sleep:
                items.append(sleep)
                tbl_sleep.append(sleep)
            
        if len(items) > 0:
            dynamo.batch_write("tbl_sleep", items)
        
        return tbl_sleep
                         
    def get_tbl_heart(self):       
        query_datetime = DATETIME_NOW.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=BASE_PERIOD)
        return dynamo.query_by_datetime(self.m_user["line_mid"], "tbl_heart", "dateTime", query_datetime.strftime("%Y-%m-%d"))

    def update_tbl_heart(self):
    
        query_datetime = DATETIME_NOW.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=BASE_PERIOD)
        tbl_heart = self.get_tbl_heart()
        
        if len(tbl_heart) == 0:
            hearts = self.time_series("activities/heart", base_date=query_datetime.strftime("%Y-%m-%d"), end_date=DATETIME_NOW.strftime("%Y-%m-%d"))
        
        else:
            df_heart = pd.DataFrame.from_dict(tbl_heart)
            max_dateTime = df_heart["dateTime"].max()
            hearts = self.time_series("activities/heart", base_date=max_dateTime, end_date=DATETIME_NOW.strftime("%Y-%m-%d"))
        
        items = []
        for heart in hearts["activities-heart"]:
            heart["line_mid"] = self.m_user["line_mid"]
            if heart not in tbl_heart:
                items.append(heart)
                tbl_heart.append(heart)
        
        if len(items) > 0:
            dynamo.batch_write("tbl_heart", items)

        return tbl_heart
        
    def get_tbl_activities(self):
        query_datetime = DATETIME_NOW.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=BASE_PERIOD)
        return dynamo.query_by_datetime(self.m_user["line_mid"], "tbl_activities", "dateTime", query_datetime.strftime("%Y-%m-%d"))

    def __get_activity_items(self, activity_names, base_date, end_date):
        
        df = pd.DataFrame()
        for name in activity_names:
            res = self.time_series("activities/{}".format(name),base_date=base_date,end_date=end_date)
            tmp = pd.DataFrame(res["activities-{}".format(name)]).rename(columns={"value":name})
            if df.shape[1] == 0:
                df = tmp.copy()
            else:
                df = df.merge(tmp, on="dateTime") 
        df["line_mid"] = self.m_user["line_mid"]            
        items = df.to_dict(orient="records")
        
        return items

    def update_tbl_activities(self):
    
        query_datetime = DATETIME_NOW.replace(hour=0, minute=0, second=0, microsecond=0) - datetime.timedelta(days=BASE_PERIOD)
        tbl_activities = self.get_tbl_activities()
        
        if len(tbl_activities) == 0:
            items = self.__get_activity_items(self.TBL_ACTIVITIES, query_datetime.strftime("%Y-%m-%d"), DATETIME_NOW.strftime("%Y-%m-%d"))
            
        else:
            df_activities = pd.DataFrame.from_dict(tbl_activities)
            max_dateTime = df_activities["dateTime"].max()
            items = self.__get_activity_items(self.TBL_ACTIVITIES, max_dateTime, DATETIME_NOW.strftime("%Y-%m-%d"))
            
            items = [item for item in items if item not in tbl_activities]
            for item in items:
                if item["dateTime"] == max_dateTime:
                    tbl_activities = [item for item in tbl_activities if item["dateTime"] != max_dateTime]
                    break

        tbl_activities.extend(items)

        if len(items) > 0:
            dynamo.batch_write("tbl_activities", items)

        return tbl_activities


class FitbitAuthController:
    
    def __init__(self, line_mid):
        self.line_mid = line_mid
        
    def handle_error(self, error):

        if error == "access_denied":
            text = "連携を許可してね。"
            line_push(self.line_mid, text)
            return True

        if error:            
            log_error(1)
            return True
        
        return False            
        

    def handle_code(self, code):
        
        if not code:
            log_error(2)
            return False

        res = self.__auth_request(code)
        if not res:
            log_error(3)
            return False        
        logger.info("[FITBIT_AUTH_RES]:{}".format(res.content))
        
        content = json.loads(res.content.decode('utf-8'))
        scopes = content["scope"].split(" ")
        if set(scopes) != set(FITBIT_SCOPES.split(" ")):
            text = "Fitbitとうまく連携できないよ。Fitbitのホームページ管理画面から後で削除もできるので、全てにチェックを入れて登録してみて。"
            line_push(self.line_mid, text)
            return False

        self.register(content)
        text = "Fitbitと連携できたよ！"
        line_push(self.line_mid, text)
        return True
        
            
    def __auth_request(self, code):
        
        headers = {
            'Authorization': 'Basic {}'.format(base64.encodebytes("{}:{}".format(FITBIT_CLIENT_ID, FITBIT_CLIENT_SECRET).encode("utf8")).decode("ascii").replace("\n","")),
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        data = [
          ('clientId', FITBIT_CLIENT_ID),
          ('grant_type', 'authorization_code'),
          ('redirect_uri', FITBIT_REDIRECT_URI),
          ('code', code),
        ]
        url = "https://api.fitbit.com/oauth2/token"

        logger.info("[FITBIT_AUTH_REQ]:{{url:{},headers:{},data:{}}}".format(url,headers,data))    
        return requests.post(url, data=data, headers=headers) 
        
    
    def register(self, content):
        
        m_user = {"line_mid":self.line_mid,
                  "fitbit_id":content["user_id"],
                  "access_token":content["access_token"],
                  "refresh_token":content["refresh_token"],
                  "scope":content["scope"],
                  "expires_in":content["expires_in"],
                  "expires_at":content["expires_at"],
                  }
        dynamo.put_user(m_user)


class Model:
    
    tmp = {"1010":"頭がぼーっとして思うようにうまく進まない状態。大事な判断はせず、なんとか1日をしのいで、",
"1020":"大事な判断で間違えたり、うっかり忘れおきるかもです。大事なことは決めないようにしましょう",
"1030":"気が散りやすく、思ったより集中が保ちづらい日。まとまった作業をやる場合は集中できる環境つくってから",
"1040":"たっぷり寝たけど疲れのとれ具合はまだ半分、無理は禁物。早めに大事なことはとりかかって！",
"1011":"",
"1021":"我慢がしづらい一日。苦手な仕事、気の進まないことを根気よくやるのが難しい一日。出来ることを探してしっかり消化しよう。",
"1031":"体力は十分に回復している。気持ちが続かない部分もあり、最後の詰めはあまくなりがち、意思決定の量は減らそう",
"1041":"睡眠は十分だけど万全ではなさそう。熱い飲み物、熱いシャワー、ちょっと運動などで、シャキッとしてみて",
"2010":"なかなか厳しい一日。ストレッチや軽めの運動で適宜リフレッシュしつつ乗り切って！ちょっとした思いつきでやることは失敗しやすいので控えてね",
"2020":"集中が続かずあまり気の進まない事や大変な事があるとめげそうになるかも。そこで投げ出さずもう一度トライしてみて。大事な事は早めの時間で片付けよう",
"2030":"体調、気持思っているよりよいです。それに比例して進むか難しいと思っていたことも、思った以上の成果がでます。いつもよりアクティブに活動してみましょ。",
"2040":"たっぷり寝たけど疲れのとれ具合はまだ半分、無理は禁物。早めに大事なことはとりかかって！",
"2011":"tbd",
"2021":"tbd",
"2031":"睡眠よくとれてますね。体はリフレッシュですが集中力は全開ではないので、大事な仕事は早めに着手！",
"2041":"睡眠十分で頭スッキリ！フレッシュな気持で仕事ができそう。集中力は全開ではないので、大事な仕事は早めに着手！",
"3010":"体力が限界。比較的早い時間に集中をして仕事を片付けてしまおう。夜は早めの就寝！",
"3020":"集中はまあまあできるが１６時くらいがタイムリミット。大事なことは早めに終わらせて！",
"3030":"頭はすっきり！十分なパフォーマンスは出せる日。根気強くやり抜こう",
"3040":"体も気力十分です。新しいこと難しいことどんどんチャレンジしよう！",
"3011":"tbd",
"3021":"まだデータないです。今日のところを記録にいれてね",
"3031":"まだデータないです。今日のところを記録にいれてね",
"3041":"まだデータないです。今日のところを記録にいれてね",
"3110":"寝不足と疲労でだいぶ体はしんどそう。集中力は少しありそうなので、やるべき事やって今日は早く休んで！",
"3120":"なんとかパフォーマンスは出せそう。時間を区切って大事な仕事から片付けて。夜は早めに休息を！",
"3130":"体力気力ともまずまず！やったことないことに思い切ってチャレンジしてみて。疲れまだのこっているので夜は早めに休息を！",
"3140":"睡眠十分で頭はスッキリ！でも疲れは残ってるので体調くずしやすいかも。無理は禁物",
"3111":"",
"3121":"まだデータないです。今日のところを記録にいれてね",
"3131":"まだデータないです。今日のところを記録にいれてね",
"3141":"ここ何日かよく寝てよく活動して素晴らしいですね。リフレッシュしてるので何か新しい事に手をつけてみよう。疲れまだ残っているみたいなので今日も軽い運動いれるとよいよ",
"4010":"疲労感あるかも。でも気持ちは充実してるので、少し体を動かすとパフォーマンスは多少でるはず。頑張って！",
"4020":"思ったよりはちゃんと結果を出せた。気を抜かずに積み上げていくと最後にいいことあるよ、頑張って",
"4030":"頭はすっきりしていて、ストレスに強く色々考えられる状態。難しいとおもっていることに積極チャレンジしよう",
"4040":"気力体力十分色々チャレンジしてみて！昨日の疲れが残る場合は軽めの運動で疲労回復するといいかも",
"4011":"tbd",
"4021":"まだデータないです。今日のところを記録にいれてね",
"4031":"頭はすっきり。気分良くすごせる１日。ストレスに強く色々考えられる状態。難しいとおもっていることに積極チャレンジしよう",
"4041":"気力体力十分色々！これは完璧な1日になりそう。なんでもできますよ。大事なことを思いっきりすすめて！",
"4110":"かなり疲れているご様子。今日は亀の様にじっとして明日に備えよう。体調崩しやすいから要注意！",
"4120":"ちょっとした判断ミスを悔やむかもしれませんが大丈夫それほど重大ではないです。選んだことが正解になるように動きましょう。疲れはあるので、早めに休息して！",
"4130":"よく寝てよく動いて充実してますね。色々なことチャレンジできるよ。ただ疲れは溜まってそう。休養も心がけて！",
"4140":"一晩の睡眠で頭はすっきり。やれていなかっことできるかも。でもまだ疲れは溜まっているので、油断しないで",
"4111":"tbd",
"4121":"まだデータないです。今日のところを記録にいれてね",
"4131":"まだデータないです。今日のところを記録にいれてね",
"4141":"ここ何日かいつもと違うペースですね。旅行とかですか？いつもやっていることも新鮮な気持ちでみてみよう。疲れまだ残っているみたいなので回復もしてね"
}
    
    def __init__(self, tbl_sleep, tbl_heart, tbl_activities):

        df_sleep = self.__group_df_sleep_by_date(pd.DataFrame(tbl_sleep))
        df_heart = pd.DataFrame(tbl_heart)
        df_activities = pd.DataFrame(tbl_activities)
        df = df_sleep.merge(df_heart, on=["line_mid", "dateTime"])
        df = df.merge(df_activities, on=["line_mid", "dateTime"])
        self.df = df

    def __group_df_sleep_by_date(self, df):
        keys = ["line_mid","dateOfSleep"]
        cols = ["awakeCount","awakeDuration","awakeningsCount","duration","efficiency","minutesAfterWakeup","minutesAsleep","minutesAwake","minutesToFallAsleep","restlessCount","restlessDuration"]
        return df[keys+cols].groupby(keys).sum().reset_index().rename(columns={"dateOfSleep":"dateTime"})
        
    def predict(self):
                
        df = self.df.sort_values("dateTime").reset_index(drop=True)
        df = df.loc[len(df)-30:, ["duration","minutesLightlyActive","minutesFairlyActive","minutesVeryActive"]]
        df = df.astype(float)

        df["activity_idx"] = df["minutesLightlyActive"] + 2*df["minutesFairlyActive"] + 3*df["minutesVeryActive"]
        df["activity_idx_avg"] = df["activity_idx"].mean()
        df["activity_idx_std"] = df["activity_idx"].std()        
        df["idx1"] = 4 * (df["activity_idx"] > df["activity_idx_avg"] + df["activity_idx_std"])
        df["idx1"] = df["idx1"] + 3 * ((df["activity_idx"] > df["activity_idx_avg"]) & (df["idx1"] < 4))
        df["idx1"] = df["idx1"] + 2 * ((df["activity_idx"] > df["activity_idx_avg"] - df["activity_idx_std"]) & (df["idx1"] < 3))
        df["idx1"] = df["idx1"] + 1 * (df["idx1"] < 2)
        idx1 = df["idx1"].values[-1] 
        idx2 = 1*((df["idx1"].values[-4:]-2.5).dot(np.array([0.2,0.5,0.7,1])) > 1.5)
        
        df["duration_avg"] = df["duration"].mean()
        df["duration_std"] = df["duration"].std()        
        df["idx3"] = 4 * (df["duration"] > df["duration_avg"] + df["duration_std"])
        df["idx3"] = df["idx3"] + 3 * ((df["duration"] > df["duration_avg"]) & (df["idx3"] < 4))
        df["idx3"] = df["idx3"] + 2 * ((df["duration"] > df["duration_avg"] - df["duration_std"]) & (df["idx3"] < 3))
        df["idx3"] = df["idx3"] + 1 * (df["idx3"] < 2)
        idx3 = df["idx3"].values[-1] 
        idx4 = 1*((df["idx3"].values[-4:]-2.5).dot(np.array([0.2,0.5,0.7,1])) > 1.5)
        
        idx = "{}{}{}{}".format(idx1,idx2,idx3,idx4)

        return idx, self.tmp[idx]


##############################        

def lambda_handler(event, context):
    
    logger.info(event)

    return event_handler(event)


def event_handler(event):
    
    # from CloudWatch
    cloud_watch_event = event.get("CloudWatchEvent")
    if cloud_watch_event:
        # Todo
        pass
    
    # from Line Post
    events = event.get("events")
    if events:        
        for ev in events:
            line_event_handler(ev) 
        return
    
    # from Fitbit Get
    line_mid = event.get("state")
    if line_mid:
        
        fb_auth = FitbitAuthController(line_mid)        
        if fb_auth.handle_error(event.get("error")):
            return
            
        fb_auth.handle_code(event.get("code"))
        return


def line_event_handler(event):
    
    type_ = event["type"]
    if type_ != "message":
        return
        
    if event["source"]["type"] != "user" or event["message"]["type"] != "text":
        return
        
    user_id = event["source"]["userId"]
    message = event["message"]["text"]
    
    m_user = dynamo.get_m_user(user_id)
    if not m_user:
        text = "未登録の人は以下のURLからFitbit連携してね。\n\n{}&state={}".format(FITBIT_AUTH_URL, user_id)
        line_push(user_id, text)
        return
    
    # TODO
    if "おつげちゃん！" in message:
        fb = ExFitbit(m_user)
        model = Model(fb.update_tbl_sleep(),
                      fb.update_tbl_heart(),
                      fb.update_tbl_activities())
        prediction = model.predict()
        message("{}\n[指標:{}]".format(prediction[1],prediction[0]))
    
    line_push(user_id, message)

    
def line_create_message_data(text):
    
    return {
        "messages": [
            {
                "type": "text",
                "text": text
            }
        ]
    }
      
def line_reply(token, text):
    
    logger.info("[LINE_REPLY]:{}".format(text))
    data = line_create_message_data(text)
    data["replyToken"] = token
    requests.post(LINE_URL_REPLY, data=json.dumps(data), headers=LINE_HEADERS)

def line_push(to, text):
    
    logger.info("[LINE_PUSH]:{{to:{},text:{}}}".format(to, text))
    data = line_create_message_data(text)
    data["to"] = to
    requests.post(LINE_URL_PUSH, data=json.dumps(data), headers=LINE_HEADERS)

def convert_to_decimal(dict_):
    return json.loads(json.dumps(dict_), parse_float=decimal.Decimal)

dynamo = DynamoDB()
