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
import kestreler
import traceback
import datetime


#初始化全局变量
_loggerClient = logger.Logger()
DIR = config.log_path					#需要同步的日志目录，在配置文件中指定
LOG_SUFFIX = ".log"					#需要采集的日志的后缀名，即文件类型
ZK_LOG_BASEPATH = "/dsp_add_node_" + config.server_name	#该采集点在zk中的目录地址，根据配置文件中唯一的server_name来确定唯一性
LOG_STATUS_START = "0"	 				#zk节点中记录的值，此值为同步初始化的值，代表当前同步到第几行，默认为0行，在同步完一行后会修改此值

#初始化zk
handler = zookeeper.init(config.zk_host)
#增加对文件目录的监控
wm = WatchManager()
mask = IN_DELETE | IN_CREATE |IN_MODIFY   # watched events
#增加kafka的生产者
client = None
producer = None
if config.queue_type == "kafka":
    client = KafkaClient(config.kafka_host)
    producer = SimpleProducer(client)
#增加kestrel的生产者
kestrel_client = None
if config.queue_type == "kestrel":
    kestrel_client = kestreler.Kestrel()
#定时任务缓冲池的对象
#SCHED_POOL = {}
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
    lock_node = nodename + "_lock" 
    #_loggerClient.info(getnode(lock_node , ""))
    if getnode(lock_node , "")[0] == "":
        setnode(lock_node, "on")
        curRow = getnode(nodename , LOG_STATUS_START)[0]
        Timer(config.delay_second,  readReleaseFileForTimer, (DIR + "/" + filename , curRow , nodename)).start()
    else:
        #_loggerClient.info("之前已加入定时任务，不操作")
        return

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
            #print   "Create file: %s " %   os.path.join(event.path, event.name)
 
    def process_IN_DELETE(self, event):
        filename = event.name
        if filename.endswith(LOG_SUFFIX):
            nodename = getNodePathByFilename(filename)
            delnode(nodename)
            delnode(nodename + "_lock")
            #print   "Delete file: %s " %   os.path.join(event.path, event.name)
     
    def process_IN_MODIFY(self, event):
        filename = event.name
        if filename.endswith(LOG_SUFFIX):
            nodename = getNodePathByFilename(filename)
            getnode(nodename, LOG_STATUS_START)
            addSchedPool(filename)
            #print   "Modify file: %s " %   os.path.join(event.path, event.name)
'''
删除节点
'''
def delnode(nodepath):
    #_loggerClient.info(">>>start delnode '" + nodepath + "' : " + str(time.time()))
    try:
        zookeeper.delete(handler, nodepath)
    except:
        traceback.print_exc()
        myprint('\n While delenode ,"' + nodepath + '" Some error/exception occurred.')
    #_loggerClient.info(">>>end delnode '" + nodepath + "' : " + str(time.time()))
'''
修改节点
'''
def setnode(nodepath , nodeval=""):
    getnode(nodepath, nodeval)
    #_loggerClient.info(">>>start setnode '" + nodepath + "' : " + str(time.time()))
    try:
        zookeeper.set(handler, nodepath, nodeval)
    except:
        traceback.print_exc()
        myprint('\nWhile setnode ,"' + nodepath + '" Some error/exception occurred.')
    #_loggerClient.info(">>>end setnode '" + nodepath + "' : " + str(time.time()))
'''
获取一个节点的值，如该节点不存在，则强制创建，并赋值空字符串
'''
def getnode(nodepath , nodeval=""):
    #_loggerClient.info(">>>start getnode '" + nodepath + "' : " + str(time.time()))
    try:
        zookeeper.get_children(handler , nodepath , None)
    except zookeeper.NoNodeException:
        zookeeper.create(handler , nodepath , nodeval , [{"perms":0x1f,"scheme":"world","id":"anyone"}] , 0)
    except:
        traceback.print_exc()
        myprint('\nWhile getnode ,"' + nodepath + '" Some error/exception occurred.')
    #_loggerClient.info(">>>end getnode '" + nodepath + "' : " + str(time.time()))
    return zookeeper.get(handler , nodepath)

'''
函数主入口
'''
def main():
    setnode(ZK_LOG_BASEPATH, "start")
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
    _loggerClient.info("初始化完成")
    setnode(ZK_LOG_BASEPATH, "end")
'''
供定时器调用
'''
def readReleaseFileForTimer(filename , rownum , nodename):
    if getnode(ZK_LOG_BASEPATH)[0] == "start":
        filename = filename.replace(DIR + "/" , "")
        nodename = getNodePathByFilename(filename)
        delnode(nodename + "_lock")
        _loggerClient.info("'" + filename + "'" + "未初始化完成，继续延迟执行")
        addSchedPool(filename)
    else:
        readReleaseFile(filename , rownum , nodename)
        delnode(nodename + "_lock")
'''
返回指定行数后的所有内容
'''
def readReleaseFile(filename , rownum , nodename):
    try:
        file_tmp = open(filename , 'r')
        allinfo = file_tmp.read()
        checkFlag = False
        for key in config.include_reg_str : 
            if key in allinfo:
                checkFlag = True
                break
        if not checkFlag:
            _loggerClient.info(">>> file '" + filename + " not contains regstr , exit' , now :" + str(time.time()))
            return 
        file_tmp.close()
        file_tmp = open(filename , 'r')
        row_array = file_tmp.readlines()
        rownum = int(rownum)
        start_rownum = rownum
        total_file_row = len(row_array)
        if total_file_row != start_rownum:
            row_array = row_array[start_rownum : total_file_row]
        else:
            row_array = []
        _loggerClient.info(">>> start set file '" + filename + "' , now :" + str(time.time()))
        zk_addcount = 0
        for row in row_array:
            rownum += 1
            setnode(nodename, str(rownum))
            if len(row.replace("\n","")) == 0:
                continue
            if len(config.include_reg_str) == 0:
                writeToQueen(row)
                zk_addcount += 1
            else:
                for key in config.include_reg_str : 
                    if key in row:
                        writeToQueen(row)
                        zk_addcount += 1
        _loggerClient.info(">>> end set file '" + filename + "' , now :" + str(time.time()) + " , total :" + str(total_file_row) + ",start :" + str(start_rownum) + ",end :" + str(rownum) + ",write:" + str(zk_addcount))
        file_tmp.close()
    except IOError:
        _loggerClient.info("'" + filename + "'" + "已被删除，不再执行该文件解析")
    except:
        traceback.print_exc()
        myprint('\nWhile readReleaseFile ,"' + filename + '","' + str(rownum) + '","' + nodename + '" Some error/exception occurred.')

    
'''
写入队列中，目前为写入kafka
'''    
def writeToQueen(val):
    #_loggerClient.info(">>>start write kafka : " + str(time.time()))
    if config.queue_type == "kafka":
        producer.send_messages(config.kafka_topic, val)
    if config.queue_type == "kestrel":
        kestrel_client.addline(config.kestrel_queue, val)
    #_loggerClient.info(">>>end write kafka : " + str(time.time()))


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
        _loggerClient.info(buffer)
        count += buffer.count('\n')  
        thefile.close()
    except :
        traceback.print_exc()
        myprint('\nWhile readReleaseFile ,Some error/exception occurred when read file \'' + filename + '\'.')
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

'''
自定义的打印
'''
def myprint(info):
    print('[' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '] ' + info)

#主函数调用
if __name__ == '__main__':
    myprint("start mission")
    main()
