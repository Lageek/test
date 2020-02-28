# -*- coding: utf-8 -*-
"""
Created on Fri Sep 20 17:56:21 2019

@author: liuzhisheng
"""
import redis

'''
redis-py使用connection pool来管理对一个redis server的所有连接，避免每次建立、释放连接的开销。
默认，每个Redis实例都会维护一个自己的连接池。可以直接建立一个连接池，然后作为参数Redis，这样就可以实现多个Redis实例共享一个连接池。
'''
#pool = redis.ConnectionPool(host = '10.18.208.42', port = '6379')  #开发redis
pool = redis.ConnectionPool(host = '10.18.207.90', port = '19000')#测试redis
r = redis.StrictRedis(connection_pool = pool)


'''
keyDelivery    = "AD:DLV:"+ ruleID:dealID  #已投放量
keySendNum     = "AD:SEND:RID:DID:"+ ruleID:dealID #已曝光量

曝光模式 0延时曝光模式 1下发即曝光模式
对于延时曝光，读已曝光量
对于
'''





import time
import pymysql
import logging


rq = time.strftime('%Y-%m-%d-%H-%M', time.localtime(time.time()))
logfile = rq + '-redis.log'

date = rq.split('-')[0] + '-' + rq.split('-')[1] + '-' + rq.split('-')[2] #今日的日期
date = '2019-09-20'
# 创建一个logger 
logger = logging.getLogger('mylogger') 
logger.setLevel(logging.DEBUG) 
  
# 创建一个handler，用于写入日志文件 
fh = logging.FileHandler(logfile) 
fh.setLevel(logging.NOTSET) 
 
# 定义handler的输出格式 
formatter = logging.Formatter('[%(asctime)s][%(thread)d][%(filename)s][line: %(lineno)d][%(levelname)s] ## %(message)s')
fh.setFormatter(formatter) 
  
# 给logger添加handler 
logger.addHandler(fh) 





mysql_params = {'host': "10.18.216.140", 'user': "root", "password": "weHtwknX/c/kuUdnkFT5Cg==", 'port': 3306, "dbName": "adland4"}  # 开发mysql
# 打开数据库连接
db = pymysql.connect(mysql_params['host'], mysql_params['user'], mysql_params['password'], mysql_params['dbName'], mysql_params['port'])
# charset='utf8'表示字符集是utf8
# 使用 cursor() 方法创建一个游标对象 cursor
cursor = db.cursor()

start1 = time.time()
sql ="SELECT a.`RULE_ID`,b.`RULE_DATE`, b.`RULE_HOUR`,a.`DEAL_ID`,a.`EXPOSE_WAY`,a.`PRIORITY`,a.`TARGET_NUM`, b.`POSITION_TYPE`, a.`sendNum`\
FROM ad_allocation a , ad_rule b where a.`RULE_ID`=b.`RULE_ID` and b.`RULE_DATE` =" + "'" + date + "'"
cursor.execute(sql)
data = cursor.fetchall()

print("数据长度：",len(data))
start2 = time.time()
print('读mysql',start2 - start1)

# 伪造测试数据
for i in data:
    value = int(i[6])
    r.set('AD:DLV:' + str(i[0]) + ':' + str(i[3]), value)
    r.set('AD:SEND:RID:DID:' + str(i[0]) + ':' + str(i[3]), value)
    #logging.debug('AD:DLV:' + str(i[0]) + ':' + str(i[3]) + '==' + str(value))
 
start3 = time.time()  
print('写redis', start3 - start2)  
    
#从redis数据库读取投放数据，写入mysql数据库
for i in data:
    if i[2] <= int(rq.split('-')[3]): #订单的小时 前于当前小时
        if str(i[4]) == '1': #下发即曝光模式
            complete_num = r.get('AD:DLV:' + str(i[0]) + ':' + str(i[3]))
        else:
            complete_num = r.get('AD:SEND:RID:DID:' + str(i[0]) + ':' + str(i[3]))
        if complete_num is None:
            logging.error('AD:SEND:RID:DID:' + str(i[0]) + ':' + str(i[3]) + 'is not exist')
        else:
            sql_update = "update ad_allocation set COMPLETE_NUM=" + str(int(complete_num)) +" " + "where RULE_ID=" + str(i[0]) 
            try:
                cursor.execute(sql_update)
                db.commit()
            except:
                db.rollback() #发生错误后回滚
cursor.close()
db.close()

print('用时:', time.time() - start3)
