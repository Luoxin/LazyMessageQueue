# TODO:利用Redis作为消息存储防止数据丢失

from flask import Flask, request
import config

from gevent import monkey
from gevent.pywsgi import WSGIServer
monkey.patch_all()

import traceback

import json

app = Flask(__name__)
app.config.from_object(config)

Message_cache = {}

@app.route('/')
def hello_world():
    return 'Hello World!'

@app.route("/messagelen")
def messagelen():  # 获取队列的某一分区的长度
    try:
        data = dict(request.args)
        if "partition" in data:
            result = {
                "high": Message_cache[data['partition']]["high"].__len__(),
                "low": Message_cache[data['partition']]["low"].__len__(),
            }
            result["all"] = result["high"] + result["low"]
            return json.dumps(result)
        else:
            return "False"
    except:
        traceback.print_exc()
        return 'False'


@app.route('/get')
def message_get():  # 从队列获取数据
    try:
        data = dict(request.args)
        if "partition" in data:
            if "priority" in data:
                return Message_cache[data['partition']][data['priority']].pop()
            elif Message_cache[data['partition']]["high"].__len__() != 0 :
                return Message_cache[data['partition']]["high"].pop()
            else:
                return Message_cache[data['partition']]["low"].pop()
        else:
            return "False"
    except:
        traceback.print_exc()
        return 'False'

@app.route('/send')
def message_send():  # 往队列发送数据
    '''
    partition:分区
    value:值
    priority:优先级
    :return:
    '''
    try:
        data = dict(request.args)
        print(data)
        if "partition" in data and "value" in data and "priority" in data:
            if data['partition'] not in Message_cache:  # 初始化一个新的key
                Message_cache[data["partition"]] = {}
                Message_cache[data["partition"]]["high"] = []
                Message_cache[data["partition"]]["low"] = []

            Message_cache[data['partition']][data['priority']].append(data["value"])

            return "True"
        else:
            return 'False'
    except:
        traceback.print_exc()
        return 'False'


if __name__ == '__main__':
    # app.run(host="0.0.0.0", port=6090)
    http_server = WSGIServer(('0.0.0.0', 6090), app)
    http_server.serve_forever()
