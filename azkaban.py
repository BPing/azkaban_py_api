# -*- coding: UTF-8 -*-
import argparse
import json
#                              _ooOoo_
#                            o8888888o
#                            88" . "88
#                            (| -_- |)
#                             O\ = /O
#                          ____/`---'\____
#                       .   ' \\| |// `.
#                        / \\||| : |||// \
#                       / _||||| -:- |||||- \
#                        | | \\\ - /// | |
#                      | \_| ''\---/'' | |
#                       \ .-\__ `-` ___/-. /
#                   ___`. .' /--.--\ `. . __
#                 ."" '< `.___\_<|>_/___.' >'"".
#                | | : `- \`.;`\ _ /`;.`/ - ` : | |
#                   \ \ `-. \_ __\ /__ _/ .-` / /
#          ======`-.____`-.___\_____/___.-`____.-'======
#                             `=---='
#
#          .............................................
#                    佛祖保佑             永无BUG
#           佛曰:
#                    写字楼里写字间，写字间里程序员；
#                    程序人员写程序，又拿程序换酒钱。
#                    酒醒只在网上坐，酒醉还来网下眠；
#                    酒醉酒醒日复日，网上网下年复年。
#                    但愿老死电脑间，不愿鞠躬老板前；
#                    奔驰宝马贵者趣，公交自行程序员。
#                    别人笑我忒疯癫，我笑自己命太贱；
#                    不见满街漂亮妹，哪个归得程序员？
import os
import sys
import time

import requests

import az_util

from az_config import *

g_usage = """usage:  python zakaban.py [action] [argument] [argument]
 
zakaban 项目操作辅助工具
       
action:
  login    登录账号；登录后将会把session信息保存在文件中，其他action会复用
  upload   上传项目任务压缩文件
  exec     执行项目工作流
  kill     kill 执行的任务
  deploy   发布项目

arguments:：
   具体参数请使用  python zakaban.py [action] -h 查看
   
info:   
   py_version python 3.0+
"""


# azkaban api https://azkaban.github.io/azkaban/docs/latest/

class NotLoginException(Exception):
    pass


def _check_nologin_error(rsp_data):
    # '{"action":"login","message":"Invalid authentication.","status":"error"}'
    if 'action' in rsp_data.keys() and 'status' in rsp_data.keys() and rsp_data["action"] == "login" \
            and rsp_data["status"] == "error":
        print("EXCEPTION: No Login; %s" % rsp_data)
        raise NotLoginException()


def az_login(usr, pwd):
    url = "%s/?action=login" % cur_azkaban_host
    post_data = {
        "action": 'login',
        "username": usr,
        "password": pwd,
    }
    rsp = requests.post(url, post_data, None, headers=com_headers, verify=False)
    print("INFO:  [response]: status=[%d],URL=[%s],headers=[%s] data=[%s]" % (
        rsp.status_code, url, com_headers, rsp.text))
    rsp = json.loads(rsp.text)
    # print(rsp)
    session_id = rsp['session.id']
    # print("[response]: session_id[%s]" % session_id)
    return session_id


def az_fetch_running_execution_of_flow(project, flow, session_id):
    url = "%s/executor?ajax=getRunning&session.id=%s&project=%s&flow=%s" \
          % (cur_azkaban_host, session_id, project, flow)
    rsp = requests.post(url, None, None, headers=com_headers, verify=False, timeout=30)
    print("INFO: [response]: status=[%d],URL=[%s],headers=[%s] data=[%s]" % (rsp.status_code, url, com_headers, rsp))
    if rsp.status_code != 200:
        print("ERROR: [response]: status=[%d],URL=[%s],headers=[%s] data=[%s]" % (
            rsp.status_code, url, com_headers, rsp.text))
        exit(-1)
    rsp_data = json.loads(rsp.text)
    _check_nologin_error(rsp_data)
    if 'error' in rsp_data.keys():
        print("ERROR: [response]: status=[%d],URL=[%s],headers=[%s] data=[%s]" % (
            rsp.status_code, url, com_headers, rsp.text))
        exit(-1)
    if 'execIds' in rsp_data.keys():
        return rsp_data['execIds']
    return []


def az_execute_flow(project, flow, session_id, exec_params=None, disabled=None, concurrent=None, pipeline_level=None):
    url = "%s/executor?ajax=executeFlow&session.id=%s&project=%s&flow=%s" \
          % (cur_azkaban_host, session_id, project, flow)
    exec_params = [] if exec_params is None else exec_params
    if disabled is not None:
        url = "%s&disabled=%s" % (url, disabled)
    if concurrent is not None:
        url = "%s&concurrentOption=%s" % (url, concurrent)
    if pipeline_level is not None:
        url = "%s&pipelineLevel=%s" % (url, pipeline_level)
    for key in exec_params:
        url = "%s&flowOverride[%s]=%s" % (url, key, exec_params[key])
    rsp = requests.post(url, None, None, headers=com_headers, verify=False, timeout=30)
    rsp_data = json.loads(rsp.text)
    _check_nologin_error(rsp_data)
    if rsp.status_code != 200 or 'error' in rsp_data.keys():
        print("ERROR: [response]: status=[%d],URL=[%s],headers=[%s] data=[%s]" % (
            rsp.status_code, url, com_headers, rsp.text))
        exit(-1)
    exec_id = 0
    if 'execid' in rsp_data.keys():
        exec_id = rsp_data['execid']
    return exec_id


def az_cancel_execute_flow(exec_id, session_id):
    url = "%s/executor?ajax=cancelFlow&session.id=%s&execid=%s" \
          % (cur_azkaban_host, session_id, exec_id)
    rsp = requests.post(url, None, None, headers=com_headers, verify=False, timeout=30)
    print("ERROR: [response]: status=[%d],URL=[%s],headers=[%s] data=[%s]" % (
        rsp.status_code, url, com_headers, rsp.text))
    rsp_data = json.loads(rsp.text)
    _check_nologin_error(rsp_data)
    if rsp.status_code != 200 or 'error' in rsp_data.keys():
        print("ERROR: [response]: status=[%d],URL=[%s],headers=[%s] data=[%s]" % (
            rsp.status_code, url, com_headers, rsp.text))
        exit(-1)
    return True


def az_upload_project(project, file_path, session_id):
    """
     上传压缩包
    :return: project_id
    """
    files = {'file': (os.path.basename(file_path), open(file_path, 'rb'), 'application/zip')}
    url = "%s/manager?ajax=upload&session.id=%s&project=%s" % (cur_azkaban_host, session_id, project)
    post_data = {'session.id': session_id, 'project': project, 'file': files, 'ajax': 'upload'}
    rsp = requests.post(url, post_data, files=files, verify=False, timeout=30)
    if rsp.status_code != 200:
        print("ERROR: [response]: status=[%d],URL=[%s],headers=[%s] data=[%s]" % (
            rsp.status_code, url, com_headers, rsp.text))
        exit(-1)
    rsp_data = json.loads(rsp.text)
    if 'error' in rsp_data.keys():
        print("ERROR: [response]: status=[%d],URL=[%s],headers=[%s] data=[%s]" % (
            rsp.status_code, url, com_headers, rsp.text))
    project_id = 0
    if 'projectId' in rsp_data.keys():
        project_id = rsp_data['projectId']
    return project_id


# TODO:
def az_download_project(project, session_id):
    # from selenium import webdriver
    url = "%s/manager?project=%s&download=true" % (cur_azkaban_host, project)
    f = requests.get(url, cookies={'azkaban.browser.session.id': session_id}, verify=False)
    with open("%.zip" % project, "wb") as fd:
        fd.write(f.content)


# util

def cur_azkaban_host_save(host):
    fo = open("azkaban.cur.host", "w")
    fo.write(host)
    fo.close()


def cur_azkaban_host_get():
    try:
        global cur_azkaban_host
        fo = open("azkaban.cur.host", "r")
        cur_azkaban_host = fo.read(100000).strip()
        fo.close()
        return cur_azkaban_host
    except FileNotFoundError:
        print("ERROR: 请先登录")
        exit(-1)


def session_save(session_id):
    global azkaban_host_map, cur_azkaban_host
    fo = open("azkaban.%s.session" % azkaban_host_map[cur_azkaban_host], "w")
    fo.write(session_id)
    fo.close()


def session_get():
    global azkaban_host_map, cur_azkaban_host, cur_session_id
    try:
        fo = open("azkaban.%s.session" % azkaban_host_map[cur_azkaban_host], "r")
        cur_session_id = fo.read(100000).strip()
        fo.close()
        return cur_session_id
    except FileNotFoundError:
        raise NotLoginException()


def session_init(args):
    global cur_session_id
    try:
        if args.Session is not None:
            cur_session_id = args.Session
    except AttributeError:
        print("INFO: args.Session 不存在")
    return cur_session_id


# action

def action_login(sys_args):
    parser = argparse.ArgumentParser(usage='python zakaban.py login -u/--user xxx -p/--password xxx')
    parser.add_argument('-u', "--user", dest='User', type=str)
    parser.add_argument('-p', "--password", dest='Pwd', type=str)
    parser.add_argument('-t', "--type", dest='Type', type=str,
                        choices=azkaban_host_arr,
                        default='offline', help="登录的系统类型")
    args = parser.parse_args(sys_args)
    global cur_azkaban_host, azkaban_host_map_2
    cur_azkaban_host = azkaban_host_map_2[args.Type]
    session_id = az_login(args.User, args.Pwd)
    session_save(session_id)
    cur_azkaban_host_save(cur_azkaban_host)
    print("session_id=%s" % session_id)


def action_upload(sys_args):
    parser = argparse.ArgumentParser(usage='python zakaban.py upload -project cbpingtest')
    parser.add_argument('-project', "--project", dest='Project', type=str, required=True)
    parser.add_argument('-session', "--session", dest='Session', type=str, default=None)
    parser.add_argument('-file', "--file", dest='File', required=True, type=str, help="项目zip文件")
    args = parser.parse_args(sys_args)
    session_init(args)
    global cur_session_id
    project_id = az_upload_project(args.Project, args.File, cur_session_id)
    print("project_id=%s" % project_id)


def action_kill(sys_args):
    parser = argparse.ArgumentParser(usage='python zakaban.py kill ')
    parser.add_argument('-flow_all', "--flow_all", dest='FlowAll', action='store_true', help="是否关掉flow所有正在执行的任务;优先级最高")
    parser.add_argument('-ids', "--ids", dest='Ids', type=str, default=None,
                        help="需要关闭的运行ID集合; ids=\"[123,124,125]\"；开启`-flow_all`后，此参数无效")
    parser.add_argument('-project', "--project", dest='Project', default=None, type=str, help="开启 flow_all 时，必填")
    parser.add_argument('-flow', "--flow", dest='Flow', default=None, type=str, help="开启 flow_all 时，必填")
    args = parser.parse_args(sys_args)
    session_init(args)
    global cur_session_id
    if args.FlowAll:
        if args.Project is None or args.Flow is None:
            print(" please input project and flow. [project]:%s [flow]:%s" % (args.Project, args.Flow))
            exit(-1)
        exec_ids = az_fetch_running_execution_of_flow(args.Project, args.Flow, cur_session_id)
    else:
        exec_ids = json.loads(args.ids)
    if len(exec_ids):
        for i, exec_id in enumerate(exec_ids):
            print("INFO: kill [project]:%s [flow]:%s [exec_id]:%s" % (args.Project, args.Flow, exec_id))
            az_cancel_execute_flow(exec_id, cur_session_id)


def _exec_if_no_exist(args):
    global cur_session_id
    try:
        execids = az_fetch_running_execution_of_flow(args.Project, args.Flow, cur_session_id)
        if len(execids) < args.Parallel:
            flow_params = None
            if args.Params is not None:
                flow_params = dict(args.Params)
            exec_id = az_execute_flow(args.Project, args.Flow, cur_session_id, disabled=args.Disabled,
                                      concurrent="pipeline",
                                      exec_params=flow_params, pipeline_level=1)
            return exec_id
        return 0
    except NotLoginException as err:
        if args.User is not None:
            session_id = az_login(args.User, args.Password)
            session_save(session_id)
            session_get()
            return 0
        else:
            raise err


def _handle_flow_exec_time(args, cur_time):
    if args.RunTime:
        args.Params["run_time"] = time.strftime("%Y-%m-%d-%H", time.localtime(cur_time))
    else:
        args.Params["ScheduleTime"] = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(cur_time))


def _get_exec_time(args):
    if "run_time" in args.Params.keys():
        return args.Params["run_time"]
    else:
        return args.Params["ScheduleTime"]


def action_exec_flow(sys_args):
    """
     执行工作流
      命令参数
    """
    parser = argparse.ArgumentParser(
        usage='python zakaban.py exec -project cbpingtest -flow uparpu_job_base_hour_start ...')
    parser.add_argument('-project', "--project", dest='Project', type=str)
    parser.add_argument('-flow', "--flow", dest='Flow', type=str)
    parser.add_argument('-parallel', "--parallel", dest='Parallel', type=int, default=1,
                        help="工作流并行执行任务数；当超过此数量，则不启动新的执行任务")
    parser.add_argument('-session', "--session", dest='Session', type=str, default=None)
    parser.add_argument('-disabled', "--disabled", dest='Disabled', type=str, default=None,
                        help="工作流里面无需执行的job;-disabled [\"x_job\",\"b_job\"]")
    parser.add_argument("-p", "--params", action='append', type=lambda kv: kv.split("="), dest='Params',
                        default=None, help="执行工作流的额外参数配置 --params run_time=2021-08-01-04 --params bar=baz")
    # 定时任务参数
    parser.add_argument("-timer", "--timer", action='store_true', dest='Timer', help="是否启动定时任务")
    parser.add_argument("-count", "--count", type=int, default=1, dest='Count',
                        help="[定时任务参数] 设置执行次数，工作流真正调度起来执行的任务次数；当达到此次数，结束执行")
    parser.add_argument("-duration", "--duration", type=int, required=True, dest='Duration', default=1,
                        help="[定时任务参数] 工作流的 ScheduleTime 间隔时间；单位/秒")
    parser.add_argument("-scheduleTime", "--scheduleTime", type=str, dest='ScheduleTime',
                        help="[定时任务参数] 工作流的调度时间 --scheduleTime=\"2021-08-01 13:30:00\"")
    parser.add_argument("-run_time", "--run_time", action='store_true', dest='RunTime',
                        help="[定时任务参数] 开启此功能，将把调度时间当成运行时间处理;工作流的执行参数从`ScheduleTime`变成`run_time`")
    parser.add_argument("-sleep", "--sleep", type=int, default=300, dest='Sleep', help="[定时任务参数] 尝试执行间隔时间，单位/秒;默认300秒")

    # 对于长期执行的任务，登录session会过期，所以需要重新登录
    parser.add_argument("-usr", "--user", dest='User', default=None, help="[登录信息] 用户;对于长期执行的任务，登录session会过期，所以需要重新登录;")
    parser.add_argument("-pwd", "--password", dest='Password', default=None, help="[登录信息] 密码")
    parser.add_argument("-login_type", "--login_type", dest='LoginType',
                        choices=azkaban_host_arr,
                        default='offline', help="[登录信息] 登录的系统类型")

    args = parser.parse_args(args=sys_args)
    if args.Params is not None:
        args.Params = dict(args.Params)
    session_init(args)
    print(args)
    if args.Timer:  # 定时任务调度
        # 当前调度时间戳
        cur_schedule_time = 0
        if args.ScheduleTime is None:
            cur_schedule_time = time.time()
        else:
            cur_schedule_time = time.mktime(time.strptime(args.ScheduleTime, "%Y-%m-%d %H:%M:%S"))
        if args.Params is None:
            args.Params = {}
        _handle_flow_exec_time(args, cur_schedule_time)
        # 当前执行次数
        cur_count = 0
        while True:
            print("INFO: try exec.....\n")
            exec_id = _exec_if_no_exist(args)
            if exec_id > 0:
                print("INFO: exec success [exec_id]:%d [cur_count]:%s [cur_time]:%s" % (
                    exec_id, cur_count, _get_exec_time(args)))
                cur_count = cur_count + 1
                # 更新下一次调度时间
                cur_schedule_time = cur_schedule_time + args.Duration
                _handle_flow_exec_time(args, cur_schedule_time)
                print("INFO: next exec  [next_count]:%s [next_time]:%s" % (cur_count, _get_exec_time(args)))
                # az_cancel_execute_flow(exec_id, args.Session)
            if cur_count > args.Count:  # 如果已达到调度次数上限，则中止执行
                print("INFO: timer is end [count]:%s" % cur_count)
                return
            time.sleep(args.Sleep)
    else:
        exec_id = _exec_if_no_exist(args)
        print("exec_id=", exec_id)


def action_deploy(sys_args):
    parser = argparse.ArgumentParser(
        usage='python zakaban.py deploy --dir=./up_emr_job_hour ',
        description='会自动根据项目目录映射到相应的azkaban，请务必保证相应的azkaban已登录')
    parser.add_argument('-dir', "--dir", dest='Dir', type=str, required=True, help="项目目录")
    parser.add_argument('-deploy', "--deploy", action='store_true', dest='Deploy', help="是否发布；否则只打包项目")
    parser.add_argument('-session', "--session", dest='Session', type=str, default=None)
    parser.add_argument('-mode', "--mode", dest='Mode', choices=["test", "online"], type=str, required=True,
                        help='发布模式：test|测试，online|线上；目前测试环境会发布到cbpingtest项目下')
    args = parser.parse_args(sys_args)
    dir_name = os.path.dirname(args.Dir)
    if len(dir_name) == 0:
        dir_name = '.'
    dir_key = az_util.dir_last_name(args.Dir)
    # 转化成linux下格式
    print("INFO: dos2unix")
    az_util.dos2unix(args.Dir, ['sh'])
    zip_name = ''
    if args.Mode == 'online':
        zip_name = "%s_%s.zip" % (dir_key, time.strftime("%Y.%m.%d.%H%M", time.localtime(time.time())))
    else:
        zip_name = "%s_test.zip" % dir_key
    zip_name = dir_name + '/' + zip_name
    print("INFO: make %s" % zip_name)
    az_util.make_zip(args.Dir, zip_name)
    if args.Deploy:
        session_init(args)
        global cur_session_id, cur_azkaban_host, azkaban_offline_host
        project = ''
        if args.Mode == 'online':
            # 目录和项目信息映射
            global dir_project_map
            if dir_key not in dir_project_map.keys():
                print("%s dir map is not exist" % args.Dir)
                exit(-1)
            project = dir_project_map[dir_key]
            cur_azkaban_host = project["host"]
            project = project["project"]
        else:
            cur_azkaban_host = azkaban_offline_host
            project = 'cbpingtest'
        # TODO:先下载保存旧的zip文件，再上传新的zip文件
        # az_download_project(project, args.Session)
        print("INFO: upload %s to '%s'.'%s'" % (zip_name, cur_azkaban_host, project))
        project_id = az_upload_project(project, zip_name, cur_session_id)
        print("INFO: deploy success ;project_id=%s" % project_id)


if __name__ == '__main__':
    '''''
     调用方式是 python azkaban.py [action] xx xx ...
    '''
    requests.packages.urllib3.disable_warnings()
    action = sys.argv[1]
    try:
        help_tag = sys.argv[2]
    except IndexError:
        help_tag = ''
    if action != 'login' and help_tag != '-h' and help_tag != '--help':
        cur_azkaban_host_get()
        session_get()

    # action 处理
    if action == "login":
        action_login(sys.argv[2:])
    elif action == "upload":
        action_upload(sys.argv[2:])
    elif action == "deploy":
        action_deploy(sys.argv[2:])
    elif action == "exec":
        action_exec_flow(sys.argv[2:])
    elif action == "kill":
        action_kill(sys.argv[2:])
    elif action == "help":
        print(g_usage)
    elif action == "test":
        print("test")

    else:
        print("else")
