#!/usr/bin/python
#-*-coding:utf-8-*-

import config
import time
import socket
import smtplib
from email.MIMEText import MIMEText

class Client :
    def __init__(self):
        #控制信息
        self.lasttime = 0
        self.host = socket.gethostname()
        self.ip   = socket.gethostbyname(self.host)
        #邮件信息
        self.server   = config.email_server
        self.username = config.email_username
        self.password = config.email_password
        self.subject  = config.email_subject
        self.sender   = config.email_sender
        self.receiver = config.email_receiver
        self.recvlist = config.email_receiver.split(',')
    
    def dosendmail(self, msg):
        #只有邮件功能开启时才发送邮件
        if config.email_enable :
            #设置邮件内容
            message = MIMEText(msg.decode('utf-8').encode('gb2312'), 'plain', 'gb2312')
            message['Subject'] = self.subject
            message['From'] = self.sender
            message['To'] = self.receiver
            #发送邮件
            try:
                smtp = smtplib.SMTP()
                smtp.connect(self.server)
                smtp.login(self.username, self.password)
                smtp.sendmail(self.sender, self.recvlist, message.as_string())
                smtp.quit()
                print "send mail succeed"
            except Exception,ex:
                print "send mail error : \n", ex
    
    #控制邮件频率（最多每5分钟发一次）
    def sendmail(self, msg):
        timenow = time.time()
        if int(timenow) >= self.lasttime :
            #控制邮件频率
            self.lasttime = int(timenow) + 5*60
            #补充内容信息
            timestr = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timenow))
            message = "\n [time] : " + timestr + \
                      "\n [host] : " + self.host + " : " + self.ip + \
                      "\n [info] : " + str(msg)
            #发送邮件
            self.dosendmail(message)
    

#测试
if __name__ == '__main__':
    client = Client()
    client.sendmail("TEST22 ...")
    client.sendmail("TEST2 ...")
