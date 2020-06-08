# _*_ coding:utf-8 _*_
import requests
import datetime
import argparse
import re
import time
import json
import pymongo as pm

# 实现了 yarn 的监控报警

# yarn http 接口
test_url = "http://address:port/ws/v1/cluster/apps"
prod_url = "http://address:port/ws/v1/cluster/apps"
# mongo auth
addr = "mongodb://user:pass@address:port/db_name"
# 企业微信机器人发送接口
wechat_url = "****************"

now = datetime.datetime.now().strftime('%Y-%m-%d')
parser = argparse.ArgumentParser(description='请依次传入筛选时间，application状态，结束状态。若无参数默认为查找今天的失败任务')

parser.add_argument("filter_date", help='筛选时间', nargs='?', type=str, default=now)
parser.add_argument("start_status", help='筛选状态', nargs='?', type=str, choices=['failed', 'killed', 'finished'],
                    default='failed')
parser.add_argument("final_status", help='筛选最终状态', nargs='?', type=str, choices=['failed', 'killed', 'succeeded'],
                    default='failed')
parser.add_argument("--mode", help='运行模式', nargs='?', dest="mode", choices=['manual', 'auto'], type=str,
                    default='manual')

arg = parser.parse_args()

if len(arg.filter_date) != 10:
    ex = Exception("Date 类型字符串错误,你的传入值为'{par}' 格式实例：2019-01-01".format(par=arg.filter_date))
    raise ex
mat = re.match(r"(\d{4}-\d{1,2}-\d{1,2})", arg.filter_date)
if mat is None:
    ex_2 = Exception("Date 类型字符串格式错误,你的传入值为'{par}' 格式实例：2019-01-01".format(par=arg.filter_date))
    raise ex_2

filter_date = arg.filter_date
start_status = arg.start_status
final_status = arg.final_status
mode = arg.mode

print("filter_date : {filter_date} \nstart_status : {start_status} || final_status : {final_status}"
      .format(filter_date=filter_date, start_status=start_status, final_status=final_status))
print("start check ！！")

time.sleep(3)


# 获得 yarn HTTP api 提供的json数据
def get_json_array(date, start_stat, final_stat):
    reponse = requests.get(prod_url)
    request_json = reponse.json()
    result_list = request_json["apps"]["app"]
    # result_list = np.array(request_json["apps"]["app"])
    w_list = []
    index = 0
    for app_stat in result_list:
        if timestamp_to_date(app_stat['finishedTime']) > format_str_to_date(date):
            if str(app_stat['state']).lower() == start_stat or str(app_stat['finalStatus']).lower() == final_stat:
                if app_stat['name'] == 'yuejuandaping':
                    continue
                else:
                    index += 1
                    result_dict = {
                        "id": str(index),
                        "application_id": app_stat['id'],
                        "user": app_stat['user'],
                        "name": app_stat['name'],
                        "queue": app_stat['queue'],
                        "state": app_stat['state'],
                        "finalStatus": app_stat['finalStatus'],
                        "startedTime": timestamp_to_datetime(app_stat['startedTime']),
                        "finishedTime": timestamp_to_datetime(app_stat['finishedTime'])
                    }

                    print("Result-{index} : {o}".format(index=index, o=result_dict))
                    w_list.append(result_dict)

    return w_list


def timestamp_to_date(timestamp):
    date = datetime.datetime.fromtimestamp(timestamp / 1000)

    return date


def timestamp_to_datetime(timestamp):
    ts = datetime.datetime.fromtimestamp(timestamp / 1000)
    date_str = datetime.datetime.strftime(ts, '%Y-%m-%d %H:%M:%S:%f')

    return date_str


def format_str_to_date(str):
    if len(str) == 10:
        date = datetime.datetime.strptime(str, '%Y-%m-%d')
    else:
        date = datetime.datetime.strptime(str, '%Y-%m-%d %H:%M:%S:%f')

    return date


def write_to_file(result):
    with open("res.txt", "w") as fp:
        fp.write(json.dumps(result, indent=4))

    fp.close()


def write_to_mongo(lists, collection):
    client = pm.MongoClient(addr)
    db = client.get_database('bd_stat')
    doc = db[collection]
    if len(lists) != 0:
        doc.insert_many(lists)

    client.close()


def find_max_time():
    max_time = ""
    collection = 'YarnMonitor'
    client = pm.MongoClient(addr)
    db = client.get_database('bd_stat')
    doc = db[collection]
    result_doc = doc.find().sort("finishedTime", -1).limit(1)
    for d in result_doc:
        max_time = d['finishedTime']

    return max_time


def wechat_notice(json_list):
    headers = {"Content-Type": "application/json;charset=utf8"}
    for js in json_list:
        data = json.dumps(js, indent=4, cls=MyEncoder)
        message = {"msgtype": "text",
                   "text":
                       {
                           "content": filter_date + "-- YarnMonitorResult:\n" + data,
                           "mentioned_mobile_list": ["@all"]
                       }}
        m = json.dumps(message, indent=4)

        requests.post(wechat_url, headers=headers, data=m)


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')


if __name__ == '__main__':
    f_date = ''
    if mode == 'auto':
        if find_max_time() == '':
            f_date = filter_date
        else:
            f_date = find_max_time()

        print(f_date)
        r = get_json_array(f_date, start_status, final_status)
        write_to_mongo(r, 'YarnMonitor')
        wechat_notice(r)
    elif mode == 'manual':
        r = get_json_array(filter_date, start_status, final_status)
        write_to_file(r)
