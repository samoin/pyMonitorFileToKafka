#!/usr/bin/ python
#-*-coding:utf-8-*-

import socket
import config
import mailer
import logging
import logging.handlers

class Logger:
    
    #这里必须是静态变量，因为全局只有一个logging
    _logger = logging.getLogger()
    _logger.setLevel(config.log_root_level)
    #设置日志内容格式
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
    #设置终端Handler
    if config.log_console_enable :
        handler_console = logging.StreamHandler()
        handler_console.setFormatter(formatter)
        handler_console.setLevel(config.log_console_level)
        _logger.addHandler(handler_console)
    #设置文件Handler
    if config.log_rotfile_enable :
        filename = config.log_rotfile_file
        
        #TimedRotatingFileHandler的构造函数定义如下（2.5版本API为例）：
        #TimedRotatingFileHandler(filename [,when [,interval [,backupCount]]])
        #filename 是输出日志文件名的前缀
        #when 是一个字符串的定义如下：
        #“S”: Seconds
        #“M”: Minutes
        #“H”: Hours
        #“D”: Days
        #“W”: Week day (0=Monday)
        #“midnight”: Roll over at midnight
        #interval 是指等待多少个单位when的时间后，Logger会自动重建文件，当然，这个文件的创建
        #取决于filename+suffix，若这个文件跟之前的文件有重名，则会自动覆盖掉以前的文件，所以
        #有些情况suffix要定义的不能因为when而重复。
        #backupCount 是保留日志个数。默认的0是不会自动删除掉日志。若设10，则在文件的创建过程中
        #库会判断是否有超过这个10，若超过，则会从最先创建的开始删除。
        handler_rotfile = logging.handlers.TimedRotatingFileHandler(filename, "D", 1)
        handler_rotfile.setFormatter(formatter)
        handler_rotfile.setLevel(config.log_rotfile_level)
        _logger.addHandler(handler_rotfile)
    
    def __init__(self):
        self.mailClient = mailer.Client()
    
    def info(self, msg):
        self._logger.info(msg)
    
    def warn(self, msg):
        self._logger.warn(msg)
    
    def debug(self, msg):
        self._logger.debug(msg)
    
    def error(self, msg):
        self._logger.error(msg)
        self.mailClient.sendmail(msg)

#测试
if __name__ == '__main__':
    client = Logger()
    client.debug("debug msg")
    client.info("info msg")
    client.warn("warn msg")
    client.error("error msg")
    client.error("error msg 2")
    
    