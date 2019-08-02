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
def fn_setObjLable(p_objid,p_labeldesc):
    objid=p_objid
    labeldesc=p_labeldesc
    #标签ID对象前缀
    prefix_for_label='L'
    #获取连接信息
    conn_str=fn_getSysDb_connectInfo
    db=cx_Oracle.connect(conn_str)
    #定义标签信息的列表
    label_info=[]
    param_sql_text1_1={'p_objid':",".join(objid)}
    #获取标签ID最大序号
    sql_text1_1='''select nvl(max(substr(c_labelid,instr(c_labelid,:p_separtor_for_id,2)+1)),0) from SYSTEM_BI_PATHTREE where c_objid=:p_objid'''
    #插入标签信息
    sql_text1_2='''insert into SYSTEM_BI_LABELS
                    (c_objid,c_labelid,c_labeldesc,c_status,d_update)
                    values
                    (:p_objid,:p_labelid,:p_labeldesc,:p_status,:p_update) '''

    cr1=db.cursor()
    cr1.execute(sql_text1_1,param_sql_text1_1)
    max_idnum=cr1.fetchone()
    max_idnum=max_idnum+1
    #sql_text1_1参数设置
    #对象ID
    label_info.append(objid)
    #标签ID
    label_info.append(prefix_for_label+p_separtor_for_id+str(objid)+p_separtor_for_id+str(max_idnum))
    #标签内容
    label_info.append(labeldesc)
    #状态0-生效;1-失效;2-禁用
    label_info.append('0')
    #更新时间
    label_info.append(format_date)
    
    cr1.execute(sql_text1_2,label_info)
    db.commit()
    db.close()
#返回对象的属性列表,用于设置标签属性时返回对象的所属属性
def fn_updateObjLable(p_labelid,**args):
    labelid=p_labelid
    labeldesc=args.pop('p_labeldesc')
    
    #获取连接信息
    conn_str=fn_getSysDb_connectInfo
    db=cx_Oracle.connect(conn_str)
    update_info=[]
    where_info=[]    
    
    sql_text1_1='''update SYSTEM_BI_LABELS 
                      set c_labeldesc=:p_labeldesc,
                          d_update   =:p_update
                    where c_labelid  =:p_labelid'''
    update_info.append(labeldesc)
    update_info.append(format_date)
    
    where_info.append(labelid)
    cr1=db.cursor()
    cr1.execute(sql_text1_1,update_info+where_info)
    db.commit()
    db.close()    
def fn_getObjLable1(p_objid):
    objid=p_objid
    #获取连接信息
    conn_str=fn_getSysDb_connectInfo
    db=cx_Oracle.connect(conn_str)
    param_sql_text1_1={'p_objid':",".join(objid)}
    sql_text1_1='''select c_objid,c_labelid,c_labeldesc,c_status,d_update 
                     from SYSTEM_BI_LABELS
                    where c_objid=:p_objid'''
    cr1=db.cursor()
    cr1.execue(sql_text1_1,param_sql_text1_1)
    objLabelList=cr1.fetch()
    
    db.close()
    
    return objLabelList
def fn_getObjLable2(p_labelid):
    labelid=p_labelid
    #获取连接信息
    conn_str=fn_getSysDb_connectInfo
    db=cx_Oracle.connect(conn_str)
    param_sql_text1_1={'p_labelid':",".join(labelid)}
    sql_text1_1='''select c_objid,c_labelid,c_labeldesc,c_status,d_update 
                     from SYSTEM_BI_LABELS
                    where c_labelid=:p_labelid'''
    cr1=db.cursor()
    cr1.execue(sql_text1_1,param_sql_text1_1)
    objLabelList=cr1.fetch()
    
    db.close()
    
    return objLabelList

def fn_getPrepOfObj(p_objid):
    objid=p_objid
    conn_str=fn_getSysDb_connectInfo
    db=cx_Oracle.connect(conn_str)
    
    cr1=db.cursor()
    param_sql_text1_1={'p_objid':",".join(objid)}
    sql_text1_1='''select c_objid,c_prepid,c_prepdesc,c_dictitemno
                     from SYSTEM_BI_RELATEPREPINFO 
                    where c_objid=:p_objid 
                      and c_status='0' '''
    cr1.execue(sql_text1_1,param_sql_text1_1)
    objPrepList=cr1.fetch()
    
    db.close()
    
    return objPrepList

    
def fn_setLabel_logicIndex(p_objid,p_prepid,p_labelid,p_conditiondesc):
    objid=p_objid
    prepid=p_prepid
    labelid=p_labelid
    #条件ID前缀
    prefix_for_labelCondition='C'
    #获取连接信息
    conn_str=fn_getSysDb_connectInfo
    db=cx_Oracle.connect(conn_str)
    param_sql_text1_1={'p_objid':",".join(objid)}
    param_sql_text1_2={'p_prepid':",".join(prepid)}
    param_sql_text1_3={'p_labelid':",".join(labelid)}    
    sql_text1_1='''select nvl(max(substr(C_CONDITIONID,instr(C_CONDITIONID,:p_separtor_for_id,2)+1)),0)
                     from SYSTEM_BI_LABELLOGIC 
                    where c_objid=:p_objid
                      and c_prepid=:p_prepid
                      and c_labelid=:p_labelid'''
    cr1=db.cursor()
    cr1.execute(sql_text1_1,param_sql_text1_1,param_sql_text1_2,param_sql_text1_3)
    max_idnum=cr1.fetchone()
    max_idnum=max_idnum+1
    condition_info=[]
    #插入标签信息
    sql_text1_2='''insert into SYSTEM_BI_LABELLOGIC
                    (c_labelid,c_objid,c_prepid,c_conditionid,c_conditiondesc,c_status,d_update)
                    values
                    (:p_labelid,:p_objid,:p_prepid,:p_conditionid,:p_conditiondesc,:p_status,:p_update) '''

    cr1=db.cursor()
    cr1.execute(sql_text1_1,param_sql_text1_1)
    max_idnum=cr1.fetchone()
    max_idnum=max_idnum+1
    #sql_text1_1参数设置
    #标签ID
    condition_info.append(labelid)
    #对象ID
    condition_info.append(p_objid)
    #属性ID
    condition_info.append(prepid)
    #属性条件ID
    condition_info.append(prefix_for_labelCondition+p_separtor_for_id+str(objid)+p_separtor_for_id+str(prepid)+p_separtor_for_id+str(max_idnum))
    #条件内容
    condition_info.append(p_conditiondesc)
    #状态0-生效;1-失效;2-禁用
    condition_info.append('0')
    #更新时间
    condition_info.append(format_date)
    
    cr1.execute(sql_text1_2,condition_info)
    db.commit()
    db.close()  
    
def fn_setLabel_logicCondition(p_conditionid,p_relateType,p_isHasNot,p_info):
    conditionid=p_conditionid
    relateType=p_relateType
    isHasNot=p_isHasNot
    info=p_info
    #获取连接信息
    conn_str=fn_getSysDb_connectInfo
    db=cx_Oracle.connect(conn_str)
    
    condition_info=[]
    sql_text1_1='''insert into SYSTEM_BI_LABELPARAM
                   (C_CONDITIONID,C_RELATETYPE,C_ISHASNOT,C_INFO,C_STATUS,D_UPDATE)
                   values
                   (:P_CONDITIONID,:P_RELATETYPE,:P_ISHASNOT,:P_INFO,:P_STATUS,:P_UPDATE)'''
    #属性条件ID
    condition_info.append(conditionid) 
    #关系运算符类型
    condition_info.append(relateType)
    #是否NOT
    condition_info.append(isHasNot)
    #条件内容
    condition_info.append(info)
    #状态0-生效;1-失效;2-禁用
    condition_info.append('0')
    #更新日期
    condition_info.append(format_date)
    
    cr1=db.cursor()
    cr1.execute(sql_text1_1,condition_info)
    db.commit()
    db.close()
#设置条件分组信息
def fn_setLable_conditionGroup(p_labelid,p_groupDesc,p_groupRelateTyp):
    labelid=p_labelid
    groupdesc=p_groupDesc
    #组关系类型1-and;2-or
    groupRelateTyp=p_groupRelateTyp
    #分组ID前缀
    prefix_for_labelConditionGroup='CG'
    #获取连接信息
    conn_str=fn_getSysDb_connectInfo
    db=cx_Oracle.connect(conn_str)
    param_sql_text1_1={'p_labelid':",".join(labelid)}
   
    sql_text1_1='''select nvl(max(substr(C_GROUPID,instr(C_GROUPID,:p_separtor_for_id,2)+1)),0)
                     from SYSTEM_BI_LABELPREP_GROUP 
                    where C_LABELID=:p_labelid'''
    cr1=db.cursor()
    cr1.execute(sql_text1_1,param_sql_text1_1)
    max_idnum=cr1.fetchone()
    max_idnum=max_idnum+1
    group_info=[]
    #插入分组信息
    sql_text1_2='''insert into SYSTEM_BI_LABELPREP_GROUP
                    (c_labelid,c_groupid,c_groupdesc,l_order,c_grouprelatetyp,c_status,d_update)
                    values
                    (:p_labelid,:p_groupid,:p_groupdesc,:p_order,:p_grouprelatetyp,:p_status,:p_update) '''
    cr1=db.cursor()
    cr1.execute(sql_text1_1,param_sql_text1_1)
    max_idnum=cr1.fetchone()
    max_idnum=max_idnum+1
    #sql_text1_1参数设置
    #标签ID
    group_info.append(labelid)
    #对象ID
    group_info.append(prefix_for_labelConditionGroup+p_separtor_for_id+str(labelid)+p_separtor_for_id+str(max_idnum))
    #属性ID
    group_info.append(groupdesc)
    #属性条件ID
    group_info.append(max_idnum)
    #条件内容
    group_info.append(groupRelateTyp)
    #状态0-生效;1-失效;2-禁用
    group_info.append('0')
    #更新时间
    group_info.append(format_date)
    
    cr1.execute(sql_text1_2,group_info)
    db.commit()
    db.close()

def fn_setLable_conditionRelate(p_labelid,p_relateObjid,p_conditionGroupid,p_relatetyp):
    labelid=p_labelid
    relateObjid=p_relateObjid
    conditionGroupid=p_conditionGroupid
    relatetyp=p_relatetyp

    #获取关联对象类型
    #获取ID分隔符第一次出现的位置
    separtor_postion=p_relateObjid.find(p_separtor_for_id)
    relateObjTyp=p_relateObjid[0,separtor_postion]

    #获取连接信息
    conn_str=fn_getSysDb_connectInfo
    db=cx_Oracle.connect(conn_str)

    param_sql_text1_1={'p_labelid':",".join(labelid)}
    param_sql_text1_2={'p_groupid':",".join(conditionGroupid)}    
    sql_text1_1='''select nvl(max(C_INNERID),0)
                     from SYSTEM_BI_LABELPREP_RELATE 
                    where C_LABELID=:p_labelid
                      and C_GROUPID=:p_groupid'''
    cr1=db.cursor()
    cr1.execute(sql_text1_1,param_sql_text1_1)
    max_idnum=cr1.fetchone()
    max_idnum=max_idnum+1
    relate_info=[]
    #插入分组信息
    sql_text1_2='''insert into SYSTEM_BI_LABELPREP_RELATE
                    (c_labelid,c_relateobjid,c_relateobjtyp,c_groupid,c_innerid,c_innerrelatetyp,c_status,d_update)
                    values
                    (:p_labelid,:p_relateobjid,:p_relateobjtyp,:p_groupdesc,:p_innerid,:p_innerrelatetyp,:p_status,:p_update) '''
    cr1=db.cursor()
    cr1.execute(sql_text1_1,param_sql_text1_1,param_sql_text1_2)
    max_idnum=cr1.fetchone()
    max_idnum=max_idnum+1
    #sql_text1_1参数设置
    #标签ID
    relate_info.append(labelid)
    #对象ID
    relate_info.append(relateObjid)
    #对象类型--为ID的前缀
    relate_info.append(relateObjTyp)    
    #属性ID
    relate_info.append(conditionGroupid)
    #组内序号
    relate_info.append(max_idnum)
    #条件内容
    relate_info.append(relatetyp)
    #状态0-生效;1-失效;2-禁用
    relate_info.append('0')
    #更新时间
    relate_info.append(format_date)

    cr1.execute(sql_text1_2,relate_info)
    db.commit()
    db.close()
#更新关联关系
def fn_updateLabel_conditionRelate(p_labelid,p_relateObjid,p_conditionGroupid,**args):
    labelid=p_labelid
    relateObjid=p_relateObjid
    conditionGroupid=p_conditionGroupid
    innerid=args.pop('p_innerid')
    innerrelatetyp=args.pop('p_innerrelatetyp')
    #获取连接信息
    conn_str=fn_getSysDb_connectInfo
    db=cx_Oracle.connect(conn_str)
    update_info=[]
    where_info=[]

    sql_text1_1='''update SYSTEM_BI_LABELPREP_RELATE
                      set c_innerid=:p_innerid,
                          c_innerrelatetyp=:p_innerrelatetyp,
                          d_update=:p_update
                    where c_labelid=:p_labelid
                      and c_relateobjid=:p_relateobjid
                      and c_groupid=:p_groupid'''

    update_info.append(innerid)
    update_info.append(innerrelatetyp)
    update_info.append(format_date)

    where_info.append(labelid)
    where_info.append(relateObjid)
    where_info.append(conditionGroupid)
    
    cr1=db.cursor()
    cr1.execute(sql_text1_1,update_info+where_info)
    db.commit()
    db.close()