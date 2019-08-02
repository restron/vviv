#coding:utf-8

import cx_Oracle
import datetime
from public_function.public_function import fn_getSysDb_connectInfo
from public_function.public_function import fn_getSepartor_forId

#获取当前系统时间
now_time = datetime.datetime.now()
#将当前系统时间转换成字符格式
format_date=datetime.datetime.strftime(now_time,'%Y-%m-%d %H:%M:%S')
#全局变量-ID层级分隔符
p_separtor_for_id=fn_getSepartor_forId()
#设置对象规则索引信息
def fn_setRoelIndex(p_roletype,p_objid,p_prepid,p_path,p_requestobj):
    roletype  =p_roletype
    objid     =p_objid
    prepid    =p_prepid
    path      =p_path
    requestobj=p_requestobj

    #规则ID对象前缀
    prefix_for_role='R'
    #获取连接信息
    conn_str=fn_getSysDb_connectInfo
    db=cx_Oracle.connect(conn_str)
    #定义标签信息的列表
    role_info=[]
    param_sql_text1_1={'p_objid':",".join(objid)}
    #获取标签ID最大序号
    sql_text1_1='''select nvl(max(substr(C_ROLEID,instr(C_ROLEID,:p_separtor_for_id,2)+1)),0) from SYSTEM_BI_ROLEINDEX where c_objid=:p_objid'''
    #插入标签信息
    sql_text1_2='''insert into SYSTEM_BI_ROLEINDEX
                    (c_roletype,c_roleid,c_objid,c_prepid,c_path,c_requestobj,c_status,d_update)
                    values
                    (:p_roletype,:p_roleid,:p_objid,:p_prepid,:p_path,:p_requestobj,:p_status,:p_update) '''

    cr1=db.cursor()
    cr1.execute(sql_text1_1,param_sql_text1_1)
    max_idnum=cr1.fetchone()
    max_idnum=max_idnum+1
    #sql_text1_1参数设置
    #对象ID
    role_info.append(roletype)
    #规则ID
    role_info.append(prefix_for_role+p_separtor_for_id+str(objid)+p_separtor_for_id+str(max_idnum))
    #对象ID
    role_info.append(objid)
    #属性ID
    role_info.append(prepid)
    #路径
    role_info.append(path)
    #请求对象
    role_info.append(requestobj)      
    #状态0-生效;1-失效;2-禁用
    role_info.append('0')
    #更新时间
    role_info.append(format_date)

    cr1.execute(sql_text1_2,role_info)
    db.commit()
    db.close()

#设置规则方案关联表
def fn_setRoleSchema(p_roleid,p_parent_path,p_requestobj,p_aftercol,p_filter,**args):
    roleid     =p_roleid
    parent_path=p_parent_path
    requestobj =p_requestobj
    aftercol   =p_aftercol
    filter     =p_filter

    defaultforother=args.pop('p_defaultforother')
    isusingmarco=args.pop('p_isusingmarco')
    isupdatebydict=args.pop('p_isupdatebydict')
    dealway=args.pop('p_dealway')
    
    #如果对象为空,则为FALSE
    if isupdatebydict:
        dicttypecode=args.pop('p_dicttypecode')
        dictitemcode=args.pop('p_dictitemcode')
        
    #规则ID对象前缀
    prefix_for_role='SCH'
    #获取连接信息
    conn_str=fn_getSysDb_connectInfo
    db=cx_Oracle.connect(conn_str)
    #定义标签信息的列表
    role_info=[]
    #获取标签ID最大序号
    sql_text1_1='''select nvl(max(substr(C_ITEMID,instr(C_ITEMID,:p_separtor_for_id,2)+1)),0) from SYSTEM_BI_TRANSATION_IDX'''
    #插入标签信息
    sql_text1_2='''insert into SYSTEM_BI_LABELS
                    (c_roletype,c_roleid,c_objid,c_prepid,c_path,c_requestobj,c_status,d_update)
                    values
                    (:p_roletype,:p_roleid,:p_objid,:p_prepid,:p_path,:p_requestobj,:p_status,:p_update) '''

    cr1=db.cursor()
    cr1.execute(sql_text1_1)
    max_idnum=cr1.fetchone()
    max_idnum=max_idnum+1
    #sql_text1_1参数设置
    #对象ID
    role_info.append(roletype)
    #规则ID
    role_info.append(prefix_for_role+p_separtor_for_id+str(objid)+p_separtor_for_id+str(max_idnum))
    #对象ID
    role_info.append(objid)
    #属性ID
    role_info.append(prepid)
    #路径
    role_info.append(path)
    #请求对象
    role_info.append(requestobj)      
    #状态0-生效;1-失效;2-禁用
    role_info.append('0')
    #更新时间
    role_info.append(format_date)

    cr1.execute(sql_text1_2,role_info)
    db.commit()
    db.close()        
#设置口径一致化接口
def fn_setTransation_IDX():
    pass
#设置口径一致化内容定义信息
def fn_setTransationDetail():
    pass
#处理口径一致化
def fn_processTransationDetail():
    pass