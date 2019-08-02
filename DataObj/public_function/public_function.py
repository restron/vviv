#coding:utf-8
#存放公用函数
#通过数据源ID获取连接字符串
def fn_getDb_connectInfo():
    DBUSER  ='fundedw'
    DBPWD   ='fundedw'
    DBIP    ='120.27.231.217'
    DBSID   ='orcl'
    return f'{DBUSER}/{DBPWD}@{DBIP}/{DBSID}'
#获取系统库连接字符串
def fn_getSysDb_connectInfo():
    DBUSER  ='fundedw'
    DBPWD   ='fundedw'
    DBIP    ='120.27.231.217'
    DBSID   ='orcl'
    return f'{DBUSER}/{DBPWD}@{DBIP}/{DBSID}'
#获取数据库对象的分隔符
def fn_getSepartor_forDb():
    return '.'
#获取ID中的分隔符
def fn_getSepartor_forId():
    return '-'
def fn_getDBType():
    pass