# -*- coding:utf-8 -*-

import datetime
import configparser
import MySQLdb

# 读取配置文件选项值
def GetCfg(section=None, option=None):
  cfg = configparser.ConfigParser()
  cfg.read('tasysinfo.ini')
  
  return cfg[section][option]

# 生成工作日信息
def GenTOpenDay(conn):
  sBeginDate = GetCfg('topenday','begindate')
  sEndDate = GetCfg('topenday','enddate')

  dBeginDate = datetime.datetime.strptime(sBeginDate, '%Y%m%d')
  dEndDate = datetime.datetime.strptime(sEndDate, '%Y%m%d')

  csr = conn.cursor()
  
  for dDay in range((dEndDate-dBeginDate).days+1):
    dDate = dBeginDate + datetime.timedelta(days=dDay)
    sWorkFlag = '0' if(dDate.weekday() == 6 or dDate.weekday() == 0) else '1';
    
    sSQL = "insert into ta_topenday(c_tenantid, c_fundcode, d_naturedate, c_agencyno, c_workflag)" + \
           " values('*', '*', " + dDate.strftime('%Y%m%d') + ",'*'," + sWorkFlag + ")"
    csr.execute(sSQL)

  conn.commit()
    


#
host = GetCfg('db', 'host')
user = GetCfg('db', 'user')
passwd = GetCfg('db', 'passwd')
db = GetCfg('db', 'dbname')

conn = MySQLdb.connect(host = host, user = user, passwd = passwd, db = db)

GenTOpenDay(conn)

conn.close()
