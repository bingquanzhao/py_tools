# -*- coding: utf-8 -*-
import os
import requests


class AzkabanModel:
    def __init__(self, conf):
        self.conf = conf
        self.url = self.conf['url']
        self.user_name = self.conf['user_name']
        self.password = self.conf['password']

    def login(self):
        login_data = {
            "action": "login",
            "username": self.user_name,
            "password": self.password
        }
        login_res = requests.post(self.url, data=login_data)
        session_id = eval(login_res.text)["session.id"]
        return session_id

    def upload_flows(self, project_name, file_path):
        session_id = self.login()
        curl_content = "curl -k -i -X POST --form " \
                       "'session.id=" + session_id + "' --form 'ajax=upload' --form " \
                                                     "'file=@" + file_path + ";type=application/zip' --form " \
                       + "'project=" + project_name + "' " + self.url + "/manager "
        os.popen(curl_content).read()
        print("上传flow成功")

    def create_project(self, project_name):
        session_id = self.login()
        create_data = {
            "session.id": session_id,
            "action": "create",
            "name": project_name,
            "description": project_name
        }
        create_res = requests.post(self.url + "/manager", data=create_data)
        if create_res.status_code == 200:
            print("create successfully")
        else:
            print("create failed")

    def delete_project(self, project_name):
        session_id = self.login()
        delete_data = {
            "session.id": session_id,
            "delete": "true",
            "project": project_name
        }
        delete_res = requests.get(self.url + "/manager", params=delete_data)
        if delete_res.status_code == 200:
            print("delete successfully")
        else:
            print("delete failed")

    def fetch_flows(self, project_name):
        session_id = self.login()
        fetch_data = {
            "session.id": session_id,
            "ajax": "fetchprojectflows",
            "project": project_name
        }
        fetch_res = requests.get(self.url + "/manager", params=fetch_data)
        if fetch_res.status_code == 200:
            print("fetch successfully")
            print(fetch_res.text)
        else:
            print("fetch failed")

    def fetch_jobs(self, project_name, flow_name):
        session_id = self.login()
        fetch_data = {
            "session.id": session_id,
            "ajax": "fetchflowgraph",
            "project": project_name,
            "flow": flow_name
        }
        fetch_res = requests.get(self.url + "/manager", params=fetch_data)
        if fetch_res.status_code == 200:
            print("fetch successfully")
            print(fetch_res.text)
        else:
            print("fetch failed")

    def schedule_flow(self, project_name, flow_name, cron_expression):
        final_id = -1
        session_id = self.login()
        schedule_data = {
            "session.id": session_id,
            "ajax": "scheduleCronFlow",
            "projectName": project_name,
            "flow": flow_name,
            "cronExpression": cron_expression
        }
        schedule_res = requests.post(self.url + "/schedule", data=schedule_data)
        if schedule_res.status_code == 200:
            # print("schedule successfully")
            r_json = eval(schedule_res.text)
            if r_json.get("error"):
                print(r_json.get("error"))
            else:
                print(schedule_res.text)
                final_id = eval(schedule_res.text)["scheduleId"]
        else:
            print("schedule failed")
        return final_id

    def unschedule_flow(self, unschedule_id):
        session_id = self.login()
        unschedule_data = {
            "session.id": session_id,
            "action": "removeSched",
            "scheduleId": unschedule_id
        }
        unschedule_res = requests.post(self.url + "/schedule", data=unschedule_data)
        if unschedule_res.status_code == 200:
            print("unschedule successfully")
        else:
            print("unschedule failed")

    # project_id而不是project_name
    def fetch_schedule(self, project_id, flow_id):
        session_id = self.login()
        fetch_data = {
            "session.id": session_id,
            "ajax": "fetchSchedule",
            "projectId": project_id,
            "flowId": flow_id
        }
        fetch_res = requests.get(self.url + "/schedule", params=fetch_data)
        if fetch_res.status_code == 200:
            print("fetch successfully")
            print(fetch_res.text)
        else:
            print("fetch failed")

    def exec_flow(self, project_name, flow_name):
        session_id = self.login()
        exec_data = {
            "session.id": session_id,
            "ajax": "executeFlow",
            "project": project_name,
            "flow": flow_name
        }
        exec_res = requests.post(self.url + "/executor", data=exec_data)
        print(exec_res.text)

    def fetch_exec_of_flow(self, project_name, flow_name, start=0, length=10):
        session_id = self.login()
        fetch_exec_data = {
            "session.id": session_id,
            "ajax": "fetchFlowExecutions",
            "project": project_name,
            "flow": flow_name,
            "start": start,
            "length": length
        }
        fetch_exec_res = requests.get(self.url + "/manager", params=fetch_exec_data)
        print(fetch_exec_res.text)

    def fetch_exec_of_flow_byid(self, execid):
        session_id = self.login()
        fetch_exec_data = {
            "session.id": session_id,
            "ajax": "fetchexecflow",
            "execid": execid
        }
        az_url = self.url
        fetch_exec_res = requests.get(az_url + "/executor?ajax=fetchexecflow", params=fetch_exec_data)
        stat = fetch_exec_res.json()['status']
        return stat
