## azkaban操作


```shell
usage:  python zakaban.py [action] [argument] [argument]

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

```