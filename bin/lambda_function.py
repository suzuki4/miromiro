# -*- coding: utf-8 -*-
"""
Created on Sat Apr 28 07:44:37 2018

@author: suzuki
"""

import requests
import json
import os
import boto3
import random
import logging
#import pandas as pd
import base64
import urllib

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

logger = logging.getLogger()
logger.setLevel(logging.INFO)


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
            
    def get_user_dict(self, line_mid):
        self.__connect_if_not()        
        table = self.con.Table("m_user")
        res = table.get_item(Key={"line_mid":line_mid})
        return res.get("Item")

    def insert_user(self, line_mid):
        self.__connect_if_not()        
        table = self.con.Table("m_user")
        return table.put_item(
                    Item={
                        "line_mid": line_mid,
                        "fitbit_id": None,
                        "token": None,
                        "refresh_token": None,
                        "scope": None
                    }
                )
dynamo = DynamoDB()


def lambda_handler(event, context):
    
    logger.info(event)

    return event_handler(event)


def event_handler(event):
    
    # from Line Post
    events = event.get("events")
    if events:        
        for ev in events:
            line_event_handler(ev) 
        return
    
    # from Fitbit Get
    line_mid = event.get("state")
    if line_mid:
        
        code = event.get("code")
        if code:
            
            if fitbit_auth_code(code):
                text = "fitbitと連携できたよ！"
            else:
                text = "fitbitとうまく連携できないよ。もう一度、運動と睡眠は必ずチェックを入れて登録してみて。"
        
        else:
            text = "fitbitとうまく連携できないよ。もう一度、登録してみて。"
        
        line_push(line_mid, text)
        return

def line_event_handler(event):
    
    type_ = event["type"]
    if type_ == "message":
        
        if event["source"]["type"] == "user" and event["message"]["type"] == "text":
            
            user_id = event["source"]["userId"]
            message = event["message"]["text"]
            
            user_di = dynamo.get_user_dict(user_id)
            if not user_di:
                text = "未登録の人は以下のURLからFitbit連携してね。\n\n{}&state={}".format(FITBIT_AUTH_URL, user_id)
                line_push(user_id, text)
                return
            
            # TODO
            line_push(user_id, message)

        
def fitbit_auth_code(code):
    
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

    res = requests.post(url, data=data, headers=headers)
    logger.info(headers)
    logger.info(data)
    logger.info(res.content)
    
    # TODO
    if res:
        return True
    return False
    
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
    
    data = line_create_message_data(text)
    data["replyToken"] = token
    requests.post(LINE_URL_REPLY, data=json.dumps(data), headers=LINE_HEADERS)

def line_push(to, text):
    
    data = line_create_message_data(text)
    data["to"] = to
    requests.post(LINE_URL_PUSH, data=json.dumps(data), headers=LINE_HEADERS)


