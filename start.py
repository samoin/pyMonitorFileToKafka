#!/usr/bin/python
#-*-coding:utf-8-*-
'''
Created on 2015年3月25日

@author: peng.wang
'''
import os
import time
import threading
import json
import config
import logger
import zookeeper
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer
from pyinotify import WatchManager, Notifier, ProcessEvent, IN_DELETE, IN_CREATE,IN_MODIFY
import sched
from threading import Timer

DIR = config.log_path					#需要同步的日志目录，在配置文件中指定
LOG_SUFFIX = ".log"					#需要采集的日志的后缀名，即文件类型
ZK_LOG_BASEPATH = "/dsp_add_node_" + config.server_name	#该采集点在zk中的目录地址，根据配置文件中唯一的server_name来确定唯一性
LOG_STATUS_START = "0"	 				#zk节点中记录的值，此值为同步初始化的值，代表当前同步到第几行，默认为0行，在同步完一行后会修改此值
#zk_row_key = "/dsp_add_row_" + config.server_name
#LOG_PREFIX = "dsp_crit_"

#初始化zk
handler = zookeeper.init(config.zk_host)
#增加对文件目录的监控
wm = WatchManager()
mask = IN_DELETE | IN_CREATE |IN_MODIFY   # watched events
#增加kafka的生产者
client = KafkaClient(config.kafka_host)
producer = SimpleProducer(client)
#定时任务缓冲池的对象
SCHED_POOL = {}
'''
根据文件名返回对应的节点路径
'''
def getNodePathByFilename(filename):
    tmpName = filename.replace(LOG_SUFFIX , "")
    nodename = ZK_LOG_BASEPATH + "/" + tmpName
    return nodename
'''
加入到定时任务缓冲池
'''
def addSchedPool(filename):
    nodename = getNodePathByFilename(filename) 
    print(SCHED_POOL)
    if SCHED_POOL.get(nodename) is None or SCHED_POOL.get(nodename) == "":
        SCHED_POOL[nodename] = "on"
        curRow = getnode(nodename , LOG_STATUS_START)[0]
	'''
        # 第一个参数确定任务的时间，返回从某个特定的时间到现在经历的秒数 
        # 第二个参数以某种人为的方式衡量时间 
        schedule = sched.scheduler(time.time, time.sleep) 
        # enter用来安排某事件的发生时间，从现在起第n秒开始启动 
        schedule.enter(config.delay_second, 0, readReleaseFile, (DIR + "/" + filename , curRow , nodename)) 
        print("加入定时任务，在" + str(config.delay_second) + "秒后执行对'" + filename + "'的执行")
        # 持续运行，直到计划时间队列变成空为止 
        schedule.run() 
	'''
        Timer(config.delay_second,  readReleaseFile, (DIR + "/" + filename , curRow , nodename)).start()
    else:
        print("之前已加入定时任务，不操作")

'''
对文件目录监控的类，从网上找的http://www.sharejs.com/codes/python/4307，用的是pyinotify模块
'''
class PFilePath(ProcessEvent):
    def process_IN_CREATE(self, event):
        filename = event.name
        if filename.endswith(LOG_SUFFIX):
            nodename = getNodePathByFilename(filename)
            getnode(nodename, LOG_STATUS_START)
            addSchedPool(filename)
        print   "Create file: %s " %   os.path.join(event.path, event.name)
 
    def process_IN_DELETE(self, event):
        filename = event.name
        if filename.endswith(LOG_SUFFIX):
            nodename = getNodePathByFilename(filename)
            zookeeper.delete(handler,nodename)
            del SCHED_POOL[nodename]
            print(SCHED_POOL)
        print   "Delete file: %s " %   os.path.join(event.path, event.name)
     
    def process_IN_MODIFY(self, event):
        filename = event.name
        if filename.endswith(LOG_SUFFIX):
            nodename = getNodePathByFilename(filename)
            getnode(nodename, LOG_STATUS_START)
            addSchedPool(filename)
        print   "Modify file: %s " %   os.path.join(event.path, event.name)
'''
获取一个节点的值，如该节点不存在，则强制创建，并赋值空字符串
'''
def getnode(nodepath , nodeval=""):
    try:
        zookeeper.get_children(handler , nodepath , None)
    except zookeeper.NoNodeException:
        zookeeper.create(handler , nodepath , nodeval , [{"perms":0x1f,"scheme":"world","id":"anyone"}] , 0)
    except:
        print '\nSome error/exception occurred.'
    return zookeeper.get(handler , nodepath)

'''
函数主入口
'''
def main():
    getnode(ZK_LOG_BASEPATH)
    readFile(config.log_path)
    watch_files()

'''
读取文件
'''
def readFile(log_path):
    #获取所有的目录下的文件，对每个文件跟zk中记录的状态进行对比，如果当前文件的行数超过记录数，则需要进行增量的导入
    listfile = os.listdir(log_path)
    for filename in listfile:
        if filename.endswith(LOG_SUFFIX):
            nodename = getNodePathByFilename(filename)
            curRow = getnode(nodename , LOG_STATUS_START)[0]
            readReleaseFile(DIR + "/" + filename , curRow , nodename)

'''
返回指定行数后的所有内容
'''
def readReleaseFile(filename , rownum , nodename):
    file_tmp = open(filename , 'r')
    row_array = file_tmp.readlines()
    rownum = int(rownum)
    if len(row_array) != rownum:
        row_array = row_array[rownum:]
    for row in row_array:
        rownum += 1
        zookeeper.set(handler, nodename, str(rownum))
        if len(row.replace("\n","")) == 0:
            continue
        if len(config.include_reg_str) == 0:
            writeToQueen(row)
        else:
            for key in config.include_reg_str : 
                if key in row:
                    writeToQueen(row)
    
'''
写入队列中，目前为写入kafka
'''    
def writeToQueen(val):
    producer.send_messages(config.kafka_topic, val)


'''
读取文件的行数
'''
def block(file,size=65536):
    while True:
        nb = file.read(size)
        if not nb:
           break
        yield nb
def getLineCount(filename):
    with open(filename,"r") as f:
        return sum(line.count("\n") for line in block(f))
            
'''
获取文件的行数,测试发现返回的都是0，不用了就
'''
def getFileRows(filename):
    count = 0  
    try:
        thefile = open(filename,'rb')  
        while True:  
            buffer = thefile.read(1024 * 8192)  
            if not buffer:  
                break  
        print(buffer)
        count += buffer.count('\n')  
        thefile.close()
    except :
        print '\nSome error/exception occurred when read file \'' + filename + '\'.'
    return count
'''
监控文件目录
'''
def watch_files():
    notifier = Notifier(wm, PFilePath())
    wdd = wm.add_watch(config.log_path, mask, rec=True)
 
    while True:
        try :
            notifier.process_events()
            if notifier.check_events():
                notifier.read_events()
        except KeyboardInterrupt:
            notifier.stop()
            break

'''
降序排序对比
'''
def compareDesc(x, y):
    stat_x = os.stat(DIR + "/" + x)
    stat_y = os.stat(DIR + "/" + y)
    if stat_x.st_ctime > stat_y.st_ctime:
        return -1
    elif stat_x.st_ctime < stat_y.st_ctime:
        return 1
    else:
        return 0
'''
升序排序对比
'''
def compareAsc(x, y):
    stat_x = os.stat(DIR + "/" + x)
    stat_y = os.stat(DIR + "/" + y)
    if stat_x.st_ctime < stat_y.st_ctime:
        return -1
    elif stat_x.st_ctime > stat_y.st_ctime:
        return 1
    else:
        return 0

#主函数调用
if __name__ == '__main__':
    main()
