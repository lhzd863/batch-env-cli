# batch-env-cli
```
批量数据运行环境
功能：
1. 支持定义系统环境变量
2. 支持提交hivesql , mysql ,postgress等存储SQL语句。
3. 小数据量 采用 命令+s 和 shell 的for循环可以支持循环操作
4. 支持作业运行结果预警提交，开始和结束打点等监控操作
5. 作业级别运行时间定义crontab,控制作业执行
6. 支持秒级时间戳的参数传入
```

# 执行作业命令
```
bash nsrpt_psql_rpt_test0000.sh NSRPT_PSQL_RPT_TEST.20170101.000000

```
