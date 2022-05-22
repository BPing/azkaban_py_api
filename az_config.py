# -*- coding: UTF-8 -*-

# azkaban的访问地址 简单的分类处理；
# 添加新的需要在下面补充上相应的映射关系
azkaban_offline_host = ""
azkaban_online_host = ""
# 支持的azkaban分类
azkaban_host_arr = ['online', 'offline']
azkaban_host_map = {
    azkaban_offline_host: "offline",
    azkaban_online_host: "online",
}
azkaban_host_map_2 = {
    "offline": azkaban_offline_host,
    "online": azkaban_online_host
}

# 会根据最新登录的系统，持久化其值到文件中
cur_azkaban_host = azkaban_offline_host
cur_session_id = ''

com_headers = {
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Requested-With": "XMLHttpRequest"
}

# 发布操作
# 发布的目录对应的项目信息
dir_project_map = {
    "emr_demo_project": {
        "project": "demo_project",
        "host": azkaban_online_host
    },
}
