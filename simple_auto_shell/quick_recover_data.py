import datetime
from pymongo import MongoClient
import time
import logging
import requests
import sys
import re

# 此脚本包括sql模式匹配替换，mongodb 数据字段更新，azkaban flow 执行 ，根据 execid 获取 azkaban flow 状态。
# 主要用于快速恢复数据


BASE_DATA = {
    'azkaban': {
        'url': '***',
        'user_name': 'azkaban',
        'password': 'azkaban'
    },
    "mongo_conf": {
        "mongo_url": "*",
        # mongo_url = "*",
        "db_name": "**",
        "collection_name": "********"
    }
}

logging.basicConfig(level=logging.INFO,
                    filename="*****************",
                    filemode='a',
                    format='%(levelname)s - %(asctime)s - %(filename)s - FUNCTION - %(funcName)s : %(message)s'
                    )


# 获取一个时间段列表 传入参数格式为 yyyy-MM-dd
def date_decrease(start, end):
    date_list = []
    begin_date = datetime.datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end, "%Y-%m-%d")
    while begin_date <= end_date:
        date_str = begin_date.strftime("%Y-%m-%d")
        date_list.append(date_str)
        begin_date += datetime.timedelta(days=1)
    return date_list


# 更新SQL中的date_key，format as 'yyyy-MM-dd'，会替换掉所有的这样格式的date字符串
def update_sql(date, task_name):
    # 基本信息
    collection_name = BASE_DATA['mongo_conf']['collection_name']
    mongo_bd_name = BASE_DATA['mongo_conf']['db_name']
    mongo_url = BASE_DATA['mongo_conf']['mongo_url']

    conn = MongoClient(mongo_url)
    db = conn[mongo_bd_name]
    collection = db[collection_name]
    # 替换
    doc = collection.find_one({"task_name": task_name})
    sql = str(doc['sql'])
    if sql.find("#") != -1:
        re_sql = sql.replace("#", date)
    else:
        re_sql = re.sub('(\\d{4}-\\d{1,2}-\\d{1,2})', date, sql)
    collection.update_one({"task_name": task_name}, {"$set": {"sql": re_sql}})
    logging.info('SQL 时间 更新为{d}'.format(d=date))
    print('SQL 时间 更新为{d}'.format(d=date))


# azkaban 登录操作
def azkaban_login():
    # 获取基础登录信息
    conf = BASE_DATA['azkaban']
    url = conf['url']
    user_name = conf['user_name']
    password = conf['password']

    login_data = {
        "action": "login",
        "username": user_name,
        "password": password
    }
    response = requests.post(url=url, data=login_data)
    session_id = response.json()["session.id"]
    logging.warning("azkaban session.id : {s}".format(s=session_id))
    return session_id


# azkaban 执行一个flow ，需要传入一个 project name 和 flow name
def exec_flow(project_name, flow_name):
    session_id = azkaban_login()
    exec_dict = {
        "session.id": session_id,
        "ajax": "executeFlow",
        "project": project_name,
        "flow": flow_name
    }
    az_url = BASE_DATA['azkaban']['url']
    exec_res = requests.post(az_url + "/executor", data=exec_dict)
    execId = exec_res.json()['execid']
    if exec_res.status_code == 200:
        logging.info("{flowname} execute success ,status_code = 200".format(flowname=flow_name))
        logging.warning("EXECID = [{execid}]".format(execid=execId))
    else:
        logging.error("{flowname} execute success ,status_code = 200".format(flowname=flow_name))
    return execId


# 获取在azkaban执行过或正在执行的flow的状态，需要传入execid
def fetch_exec_of_flow_byid(execid):
    session_id = azkaban_login()
    fetch_exec_data = {
        "session.id": session_id,
        "ajax": "fetchexecflow",
        "execid": execid
    }
    az_url = BASE_DATA['azkaban']['url']

    # 设置一个死循环，直到出现 'SUCCEEDED' 跳出循环
    while True:
        fetch_exec_res = requests.get(az_url + "/executor?ajax=fetchexecflow", params=fetch_exec_data)
        time.sleep(10)
        stat = fetch_exec_res.json()['status']
        logging.info(
            "  {id} NOW STATUS_CODE of execucting flow : %%%%   {status}   %%%% ".format(id=execid, status=stat))
        if stat == 'FAILED':
            logging.error("execute error !!! ")
            sys.exit()
        if stat == 'SUCCEEDED':
            break
    return stat


if __name__ == '__main__':
    start_time = sys.argv[1]
    end_time = sys.argv[2]
    project_name = sys.argv[3]
    flow_name = sys.argv[4]
    date_list = date_decrease(start_time, end_time)
    exporter_task = '******************'

    for d in date_list:
        update_sql(d, exporter_task)
        execId = exec_flow(project_name, flow_name)
        time.sleep(5)
        status = fetch_exec_of_flow_byid(execId)
        logging.info("{dat} **** run success ****".format(dat=d))

