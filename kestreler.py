#!/usr/bin/ python
#-*-coding:utf-8-*-

import os
import sys
import time
import config
import logger
import kestrel


class Kestrel():
    
    def __init__(self):
        self.logclient = logger.Logger()
        self.kclient = kestrel.Client(config.kestrel_server)
    
    def addline(self, queue, line) :
        line = line.rstrip()
        res = self.kclient.add(queue, line)
        return res
