#!/usr/bin/python
# -*- coding: utf-8 -*- 
# Author: memoryblade
import MySQLdb
import sys
import tempfile
import urllib2
import urllib
import json
import threading
from datetime import datetime
from springpython.database.core import *
from springpython.database.factory import *
from springpython.database.transaction import *

global LOG


def initlog():
    import logging
    logger = logging.getLogger()
    hdlr = logging.StreamHandler()
    flr = logging.FileHandler("pylog")
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    flr.setFormatter(formatter)
    logger.addHandler(hdlr)
    logger.addHandler(flr)
    logger.setLevel(logging.NOTSET)
    return (logger, flr)


class ImproveMySQLConnectionFactory(MySQLConnectionFactory):
    def __init__(self, user=None, passwd=None, host=None, port=None, db=None, charset="utf8"):
        MySQLConnectionFactory.__init__(self, username=user, password=passwd, hostname=host, db=db)
        self.charset = charset
        self.port = port

    def connect(self):
        """The import statement is delayed so the library is loaded ONLY if this factory is really used."""
        import MySQLdb
        return MySQLdb.connect(host=self.hostname, port=self.port, user=self.username, passwd=self.password, db=self.db,
                               charset=self.charset)

    def in_transaction(self):
        return True

    def count_type(self):
        return types.LongType


class DistributedDbHandler:
    def __init__(self, user, passwd, db, charset):
        self.user = user
        self.passwd = passwd
        self.db = db
        self.charset = charset
        self.connectionFactory = {}
        self.dt = {}
        # self.conn = {}
        # self.cur = {}

    def __del__(self):
        # for cur in self.cur.values():
        #     cur.close()
        #
        # for conn in self.conn.values():
        #     conn.close()
        #
        # print "closed"
        pass

    # 获取分库的序号
    def getDistributedIndex(self, key):
        return 0

    # 有可能按host或者port分库，按情况来
    def getHostAndPort(self, index):
        pass

    def execute(self, func, key, argDict):
        try:
            index = self.getDistributedIndex(key)
            if self.dt.get(index) is None:
                self.connect(index)
            (sql, resultType, rowMapper) = func(argDict)
            if types.ListType == resultType:
                return self.queryForList(sql=sql, index=index)
            elif types.NoneType == resultType:
                return self.query(sql=sql, index=index, rowMapper=rowMapper)
            else:
                return self.queryForObject(sql=sql, index=index, resultType=resultType)
        except Exception, e:
            LOG.error('Error %d: %s' % (e.args[0], e.args[1]))

    def connect(self, index):
        (host, port) = self.getHostAndPort(index)
        connFac = ImproveMySQLConnectionFactory(user=self.user, passwd=self.passwd, host=host, db=self.db, port=port)
        self.connectionFactory[index] = connFac
        self.dt[index] = DatabaseTemplate(connFac)

    def query(self, sql, index=0, rowMapper=None):
        try:
            return self.dt.get(index).query(sql, rowhandler=rowMapper)
        except Exception, e:
            LOG.error('Error %d with sql: %s' % (e.args[0], sql))
            if e.args[0] == 2006:
                try:
                    LOG.info("reconnecting...")
                    self.connect(index)
                    return self.dt.get(index).query(sql, rowhandler=rowMapper)
                except Exception, e:
                    LOG.error('Error %d with sql: %s' % (e.args[0], sql))

    def queryForObject(self, sql, index=0, resultType=types.ObjectType):
        try:
            return self.dt.get(index).query_for_object(sql, required_type=resultType)
        except Exception, e:
            LOG.error('Error %d with sql: %s' % (e.args[0], sql))
            if e.args[0] == 2006:
                try:
                    LOG.info("reconnecting...")
                    self.connect(index)
                    return self.dt.get(index).query_for_object(sql, required_type=resultType)
                except Exception, e:
                    LOG.error('Error %d with sql: %s' % (e.args[0], sql))

    def queryForList(self, sql, index=0):
        try:
            return self.dt.get(index).query_for_list(sql)
        except Exception, e:
            LOG.error('Error %d with sql: %s' % (e.args[0], sql))
            if e.args[0] == 2006:
                try:
                    LOG.info("reconnecting...")
                    self.connect(index)
                    return self.dt.get(index).query_for_list(sql)
                except Exception, e:
                    LOG.error('Error %d with sql: %s' % (e.args[0], sql))


class MyDBHandler(DistributedDbHandler):
    def __init__(self, user, passwd, db, charset):
        DistributedDbHandler.__init__(self, user, passwd, db, charset)

    # 获取分库的序号,key为ucid
    def getDistributedIndex(self, key):
        return (key / 256 * 8 + key % 6) % 16

    # 有可能按host或者port分库，按情况来
    def getHostAndPort(self, index):
        host = "192.168.0.6"
        port = 6300 + index
        return (host, port)

class AdMapper(RowMapper):
    """This will handle one row of database. It can be reused for many queries if they
       are returning the same columns."""

    def map_row(self, row, metadata=None):
        return Ad(id=row[0], name=row[1], aderId=row[2])


class Ad:
    def __init__(self, id, name, aderId):
        self.id = id
        self.name = name
        self.aderId = aderId


def getAdAbstract(argDict):
    return "SELECT ad.id,ad.name,ad.ader_id from brand_start_ad ad where ad.id=%s" % (
    str(argDict["id"])), types.NoneType, AdMapper()

def getAdAbstract1(argDict):
    return "SELECT ad.id,ad.name,ad.ader_id from brand_start_ad ad where ad.id=%s" % (
    str(argDict["id"])), types.NoneType, DictionaryRowMapper()


def getAdAderId(argDict):
    return "SELECT ad.ader_id from brand_start_ad ad where ad.id=%s" % (str(argDict["id"])), types.LongType, None


def generateInSql(len):
    return "(" + "%s," * (len - 1) + "%s)"


def main():
    dbhandler = MyDBHandler("name", "pass", "db", "utf8")

    param = {}
    param["id"] = 571715
    # 返回一个字典的列表
    resultList = dbhandler.execute(func=getAdAbstract1, key=None, argDict=param)
    # 直接包装
    resultList = dbhandler.execute(func=getAdAbstract, key=None, argDict=param)
    # 返回一个row[]的列表
    resultList = dbhandler.execute(func=getAdAbstract, key=None, argDict=param)
    # 返回一个结果
    result = dbhandler.execute(func=getAdAderId, key=None, argDict=param)


if __name__ == "__main__":
    (LOG, FLR) = initlog()
    main()
    FLR.flush()
