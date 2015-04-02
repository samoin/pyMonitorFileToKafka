#!/usr/bin/ python
#-*-coding:utf-8-*-

import sys
import logging

#servername
server_name         = "0"

#queuetype, now support ["kestrel" , "kafka"]
queue_type          = "kestrel"

# delay
#delay_second        = 3*60
delay_second        = 10

# path
log_path            = "/dsp/rsylog"

#zookeeper
zk_host             = "localhost:2181"

#kestrel
kestrel_enable      = True
kestrel_server      = ["localhost:22133"]
kestrel_queue       = "sdpp_basic_debug"

# kafka
kafka_host          = "localhost:9092"
kafka_topic	    = "dsp_rtb_log2"

# 导入的条件字符串
include_reg_str	    = ['"type":"click"' , '"type":"show"' , '"type":"push"']
#include_reg_str            = []						#不过滤日志，全部写入，默认会排除空行

# logger
log_console_enable  = True
log_rotfile_enable  = True
log_root_level      = logging.INFO
log_console_level   = logging.INFO
log_rotfile_level   = logging.INFO
log_rotfile_file    = sys.path[0]+"/logs/add.log"

# email
email_enable        = False
email_server        = 'mail.fractalist.com.cn:25'
email_subject       = '[monitor] exp debug'
email_receiver      = 'peng.wang@fractalist.com.cn'
email_sender        = 'support@fractalist.com.cn'
email_username      = 'support@fractalist.com.cn'
email_password      = 'Zcc_0221'
