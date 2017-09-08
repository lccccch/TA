# -*- coding:utf-8 -*-

import MySQLdb
import configparser

# 获取表的主键字段列表
# conn = 数据库连接 tablename = 表名 dbname = 数据库名
def GetTablePriKeyColumn(conn, tablename, dbname):
        csr = conn.cursor()
        sSql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tablename + "' AND COLUMN_KEY = 'PRI' AND TABLE_SCHEMA = '" + dbname + "'";
        csr.execute(sSql)
        return csr.fetchall()

# 获取表的非主键字段列表
# conn = 数据库连接 tablename = 表名 dbname = 数据库名
def GetTableNONPriKeyColumn(conn, tablename, dbname):
        csr = conn.cursor()
        sSql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tablename + "' AND COLUMN_KEY <> 'PRI' AND TABLE_SCHEMA = '" + dbname + "'";
        csr.execute(sSql)
        return csr.fetchall()

# 根据字段列表生成SQL语句条件,比较运算符为=,连接条件为and 
def GenEqSQLCondition(columnlist):
        cond = ""
        for column in columnlist:
                cond = cond + " and a."+column[0]+ " = " +"b."+column[0]
        return cond

# 根据字段列表生成SQL语句条件,比较运算符为<>,连接条件为or
def GenNonEqSQLCondition(columnlist):
        cond = ""
        for column in columnlist:
                cond = cond + " or a."+column[0]+ " <> "+"b."+column[0]
        return cond
# 生成select字段列表
def GenSelectColumnList(columnlist):
        sellist = ""
        i = 0
        for column in columnlist:
                i += 1
                sellist = sellist +" a."+ column[0]
                if(i != len(columnlist)):
                   sellist = sellist+","               
        return sellist

# 查询两个数据库同一张表,主键字段相等,非主键字段不等的记录
def CmpTwoTable(conn, dbname01, dbname02, tablename):

        csr = conn.cursor()
        # 比较记录数
        # 数据库01
        # print("  比较表%s记录数" % (tablename))
        sSql = "SELECT COUNT(*) FROM " + dbname01 + "." + tablename
        csr.execute(sSql)
        count01 = csr.fetchone()[0]

        # 数据库02
        sSql = "SELECT COUNT(*) FROM " + dbname02 + "." + tablename
        csr.execute(sSql)
        count02 = csr.fetchone()[0]
        if count01 != count02:
                print("  以下表记录数不一致: ")
                print("    数据库%s表%s记录数为:%d" % (dbname01, tablename, count01))
                print("    数据库%s表%s记录数为:%d" % (dbname02, tablename, count02))
        
        # 获取主键字段列表
        prilist = GetTablePriKeyColumn(conn = conn, tablename = tablename, dbname = dbname01)

        # 生成主键字段比较SQL
        sellist = GenSelectColumnList(prilist)
        condofprikey = GenEqSQLCondition(prilist)

        # 生成非主键字段比较SQL
        nonprilist = GetTableNONPriKeyColumn(conn = conn, tablename =tablename, dbname = dbname01)
        condofnoprikey = GenNonEqSQLCondition(nonprilist)

        # 数据库01表多出记录
        
        # 数据库02表多出记录
        
        # 主键字段相等,非主键字段不等的记录
        sCmpSql = "SELECT " + sellist + " FROM " + dbname01 + "." + tablename + " a," +  dbname02 + "." + tablename + " b WHERE 1 = 1" + condofprikey + " and ( 1 <> 1 " + condofnoprikey + ")"
        if (csr.execute(sCmpSql)) > 0:
                print("  表%s存在主键字段相等,非主键字段不等的记录" % tablename)
                print("  主键%s:" % sellist, csr.fetchall())

#
def CompareTableInList():
        cfgCmp = configparser.ConfigParser()
        cfgCmp.read("cmp.ini")
        for section in cfgCmp.sections():
                host = cfgCmp.get(section, "host")
                user = cfgCmp.get(section, "user")
                passwd = cfgCmp.get(section, "passwd")

                conn = MySQLdb.connect(host = host, user = user, passwd = passwd)
                
                dbname01 = cfgCmp.get(section, "dbname01")
                dbname02 = cfgCmp.get(section, "dbname02")

                print("开始比较%s数据库%s和数据库%s的表数据" % (host, dbname01, dbname02))
                
                for tablename in cfgCmp.get(section, "tables").split(','):
                        CmpTwoTable(conn, dbname01, dbname02, tablename)
                conn.close()

CompareTableInList()
