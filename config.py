#!/usr/bin/ python
#-*-coding:utf-8-*-

import sys
import logging

#thread_num
# 如果使用kafka，尽量将值设置为与kafka的partitions数量一致，目前将根据hashcode的值进行hash分片的写入
thread_num          = 3
#servername
#server_name         = "0"
server_name         = "1"
# 是否在启动时进行所有的比对
init_all            = False

#queuetype, now support ["kestrel" , "kafka"]
queue_type          = "kafka"

# delay
#delay_second        = 3*60
delay_second        = 10

# path
log_path            = "/dsp/rsylog/all"
#log_path            = "/dsp/rsylog/test"

#zookeeper
#zk_host             = "localhost:2181"
zk_host             = "localhost:12181"

#kestrel
kestrel_enable      = True
kestrel_server      = ["192.168.5.7:22133"]
kestrel_queue       = "sdpp_basic_debug_thread"

# kafka
kafka_host          = "192.168.5.81:19092,192.168.5.81:19093,192.168.5.81:19094"
kafka_topic	    = "dsp_test3"

# 导入的条件字符串
include_reg_str	    = ['"type":"click"' , '"type":"show"' , '"type":"push"']
#include_reg_str            = []						#不过滤日志，全部写入，默认会排除空行

# logger
log_console_enable  = True
log_rotfile_enable  = True
log_root_level      = logging.INFO
log_console_level   = logging.INFO
log_rotfile_level   = logging.INFO
log_rotfile_file    = sys.path[0]+"/logs/start.log"

# email
email_enable        = False
email_server        = 'mail.fractalist.com.cn:25'
email_subject       = '[monitor] exp debug'
email_receiver      = 'peng.wang@fractalist.com.cn'
email_sender        = 'support@fractalist.com.cn'
email_username      = 'support@fractalist.com.cn'
email_password      = 'Zcc_0221'
