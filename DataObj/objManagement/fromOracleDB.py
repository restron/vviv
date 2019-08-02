#coding:utf-8
import cx_Oracle
import datetime
from itertools import combinations
from public_function.public_function import *

#全局变量-数据库对象分隔符
p_separtor=fn_getSepartor_forDb()
#全局变量-ID层级分隔符
p_separtor_for_id=fn_getSepartor_forId()
#获取当前系统时间
now_time = datetime.datetime.now()
#将当前系统时间转换成字符格式
format_date=datetime.datetime.strftime(now_time,'%Y-%m-%d %H:%M:%S')

#获取对象所有路径组合
def fn_setAll_PathCombo(p_sourceid,p_objid,p_prepid,p_path,p_opertype=1):
    #路径ID前缀
    prefix_for_path='P'
    #0-数据库物理路径,1-业务逻辑虚拟路径,前者由代码实现插入,后者由页面手工插入
    opertype=p_opertype
    
    objid=p_objid
    #判断空值,如果是单一主键对象,它只会有对象ID,不会生成属性ID,需要有默认值
    if p_prepid.nonzero() and p_prepid.len()==0:
        p_prepid=0
    
    prepid=p_prepid
    #数据源ID
    sourceid=p_sourceid
    #根据数据源ID获取数据库连接信息
    conn_str=fn_getSysDb_connectInfo(sourceid)
        
    #创建数据库连接对象
    db=cx_Oracle.connect(conn_str)    
    n=p_path.count('.')
    
    temp_list = []

    for i in combinations(p_path, n+1):
        if ("".join(i).strip(p_separtor)).count(p_separtor)== 0:
            temp_list.append(("{0}".join(i)).format(p_separtor).strip(p_separtor))
     
#将需要批量插入的值统一插入至列表,它的高度由temp_list定义
    path_info   = [[]]*temp_list.len()
    for j, path_val in enumerate(temp_list, 1):
        path_info[j].append(sourceid)#数据源ID
        path_info[j].append(objid)#对象ID
        path_info[j].append(prefix_for_path+p_separtor_for_id+str(objid)+p_separtor_for_id+str(j))#路径ID
        path_info[j].append(prepid)#属性ID
        path_info[j].append(path_val)#路径值
        path_info[j].append(opertype)#路径类型
        path_info[j].append(path_val.count(p_separtor)+1)#目录个数
        path_info[j].append('0')#状态
    
    #将已有记录更新为失效
    sql_text1_1='''update SYSTEM_BI_PATHTREE 
                      set c_status='0',d
                    where c_objid=:p_objid 
                      and c_prepid=:p_prepid '''
    #插入新记录 
    sql_text1_2='''insert into SYSTEM_BI_PATHTREE
                   (c_sourceno,c_objid,c_pathid,c_prepid,c_path,c_type,l_dirnum,c_status) 
                    values
                   (:p_sourceid,:p_objid,:p_pathid,:p_prepid,:p_path,:p_type,:p_dirnum,:p_status) '''
    cr1=db.cursor()
    param_sql_text1_1={'p_objid':",".join(objid)}
    param_sql_text1_2={'p_prepid':",".join(prepid)}
    cr1.execute(sql_text1_1,param_sql_text1_1,param_sql_text1_2)
    #批量插入数据
    cr1.executemany(sql_text1_2,path_info)

    db.commit()
    db.close()
    return temp_list

#获取路径下的所有目录
def fn_setAll_DirOfPath(p_sourceid,p_objid,p_prepid):
    
    prefix_for_dir='D'
    #数据源ID
    sourceid=p_sourceid
    #对象ID
    objid=p_objid
    #属性ID
    prepid=p_prepid
    #根据数据源ID获取数据库连接信息
    conn_str=fn_getSysDb_connectInfo(sourceid)
    #创建数据库连接对象
    db=cx_Oracle.connect(conn_str)
    
    param_sql_text1_1={'p_sourceid':",".join(sourceid)}
    param_sql_text1_2={'p_objid':",".join(objid)}
    param_sql_text1_3={'p_prepid':",".join(prefix_for_dir+prepid)}
    
    #获取目录的层级,目的是为了判断当前目录所在的层级,考虑到极端情况下,每个路径的名称均相同
    sql_text1_1='''select max(l_dirnum) from SYSTEM_BI_PATHTREE where c_sourceno=:p_sourceid and c_objid=:p_objid and c_prepid=:p_prepid and c_status='0' '''    
    #插入目录信息
    sql_text1_2='''select c_objid,c_prepid,c_path from SYSTEM_BI_PATHTREE where c_sourceno=:p_sourceid and c_objid=:p_objid and c_prepid=:p_prepid and c_status='0' and l_dirnum=1'''

    cr1=db.cursor()
    cr1.execute(sql_text1_1,param_sql_text1_1,param_sql_text1_2,param_sql_text1_3)
    signledirs=cr1.fetchone()
    
    cr1.execute(sql_text1_2,param_sql_text1_1,param_sql_text1_2,param_sql_text1_3)
    singledir_list=cr1.fetch()
    #插入目录信息
    param_sql_text1_4={'p_signledirs':",".join(signledirs)}
    sql_text1_3='''select c_objid,c_prepid,c_path from SYSTEM_BI_PATHTREE where c_sourceno=:p_sourceid and c_objid=:p_objid and c_prepid=:p_prepid and c_status='0' and l_dirnum=:p_signledirs'''
    cr1.execute(sql_text1_3,param_sql_text1_1,param_sql_text1_2,param_sql_text1_3,param_sql_text1_4)
    
    full_path=cr1.fetchone()
    cr1.close()
    #获取完整的目录列表,之所以不从sql_text1_2的结果获取,是为了通过从一个完整的路径中拆分保持路径中目录的顺序
    full_path_list=full_path.split(p_separtor)

    signdirs_info=[[]]*signledirs
    for i, path_val in enumerate(singledir_list, 1):
        signdirs_info[i].append(objid)#对象ID
        signdirs_info[i].append(prepid)#属性ID
        signdirs_info[i].append(prefix_for_dir+p_separtor_for_id+str(objid)+p_separtor_for_id+str(i))#目录ID
        signdirs_info[i].append(full_path_list.index(path_val,i))#从第i个位置开始定位当前的path_val值的位置,作为目录在路径中所在的层级
        signdirs_info[i].append(format_date)#生效日期
        signdirs_info[i].append('0')#是否使用
    #更新历史记录的状态
    param_sql_text2_1={'p_date':",".join(format_date)} 
    sql_text2_1='''update SYSTEM_BI_DIR
                      set c_status='1',d_end=:p_date
                    where c_sourceno=:p_sourceid
                      and c_objid=:p_objid
                      and c_prepid=:p_prepid
                      and c_status='0' '''
    sql_text2_2='''insert into SYSTEM_BI_DIR
                   (C_OBJID,C_PREPID,C_DIRCODE,L_INDEX,D_BEGIN,C_STATUS)
                   values
                   (:p_objid,:p_prepid,:p_dircode,:p_index,:p_date,:p_status) '''
    cr2=db.cursor()
    cr2.execute(sql_text2_1,param_sql_text2_1,param_sql_text1_1,param_sql_text1_2,param_sql_text1_3)
    cr2.executemany(sql_text2_2,signdirs_info)
    
    sql_text2_3='''update SYSTEM_BI_DIR a
                      set a.C_UPDIRCODE=
                      (select b.C_DIRCODE
                         from SYSTEM_BI_DIR b
                        where a.c_sourceno=b.c_sourceno
                          and a.c_objid=b.c_objid
                          and a.c_prepid=b.c_prepid
                          and a.l_index=b.l_index+1
                          and a.c_status='0')
                    where a.c_status='0'
                      and a.c_sourceno=:p_sourceno '''
    sql_text2_4='''update SYSTEM_BI_DIR a
                      set a.C_UPDIRCODE=a.C_DIRCODE
                    where a.c_status='0'
                      and a.l_index=0
                      and a.c_sourceno=:p_sourceno '''     
    sql_text2_5='''update SYSTEM_BI_DIR a
                      set a.C_ROOTDIRCODE=
                      (select b.C_DIRCODE
                         from SYSTEM_BI_DIR b
                        where a.c_sourceno=b.c_sourceno
                          and a.c_objid=b.c_objid
                          and a.c_prepid=b.c_prepid
                          and b.l_index=0
                          and a.c_status='0')
                    where a.c_status='0'
                      and a.c_sourceno=:p_sourceno '''
    cr2.execute(sql_text2_3,param_sql_text1_1)
    cr2.execute(sql_text2_4,param_sql_text1_1)
    cr2.execute(sql_text2_5,param_sql_text1_1)
    db.commit()
    db.close()

#添加/插入 字典表信息
def fn_addNewItem(p_sourceid):
    #数据源ID
    sourceid=p_sourceid
    #根据数据源ID获取数据库连接信息
    conn_str=fn_getDb_connectInfo()
    #创建数据库连接对象
    db=cx_Oracle.connect(conn_str)    

    param_sql_text1_1={'p_sourceid':",".join(sourceid)}
    #SQL1,根据数据源ID获取对应的当前最大采集日期
    sql_text1_1='''select nvl(max(d_ddltime),to_date(''\'19700101\''',''\'yyyymmdd\''')) from SYSTEM_BI_TMPOBJS where c_sourceno=:p_sourceid and c_status='0' '''
    cr1=db.cursor()
    cr1.execute(sql_text1_1,param_sql_text1_1)
    tmp_datadate=cr1.fetchone()
    cr1.close()
    #SQL2,获取数据库日期
    sql_text2='select trunc(sysdate) from dual'
    cr2=db.cursor()
    cr2.execute(sql_text2)
    cur_datadate=cr2.fetchone()
    cr2.close()
        
    #SQL3,获取历史表数据日期
    param_sql_text3_1={'p_sourceid':",".join(sourceid)}
    #已确认的最大日期
    sql_text3_1='''select to_char(nvl(max(d_ddltime）,to_date('19700101','yyyymmdd')),'YYYYMMDD-hh24:mi:ss') from SYSTEM_BI_OBJCONFIRM where c_sourceno=:p_sourceid'''
    #未匹配的数据的最大日期
    sql_text3_2='''select to_char(nvl(max(d_ddltime）,to_date('19700101','yyyymmdd')),'YYYYMMDD-hh24:mi:ss') from SYSTEM_BI_TMPOBJS where c_sourceno=:p_sourceid and c_status='3' '''

    cr3=db.cursor()
    cr3.execute(sql_text3_1,param_sql_text3_1)
    his_datadate_for_0=cr3.fetchone()
    cr3.execute(sql_text3_2,param_sql_text3_1)
    his_datadate_for_3=cr3.fetchone()
    cr3.close()

    his_datadate=max(his_datadate_for_0,his_datadate_for_3)#考虑当日的数据全部匹配或是不匹配
        
    if   cur_datadate>tmp_datadate:
        data_status='0'#当前系统时间大于SYSTEM_BI_TMPOBJS表中的日期,为正常情况
    elif cur_datadate<=tmp_datadate:
        data_status='1'#当前系统时间小于等于SYSTEM_BI_TMPOBJS表中的日期,异常,立即退出
    elif tmp_datadate<his_datadate:
        data_status='2'#插入SYSTEM_BI_TMPOBJS为历史数据,无需处理,立即退出
    #获取当前最大的DDL时间
    param_sql_text4_1=his_datadate.__str__()[2:19]
    param_sql_text4_2=p_sourceid
    param_sql_text4_3=p_separtor
    param_sql_text4_4=p_separtor_for_id

    #SQL4,从ORACLE数据库字典表获取信息
    sql_text4_1='''truncate table SYSTEM_BI_TMPOBJS'''
    sql_text4_2='''insert into SYSTEM_BI_TMPOBJS
                   (c_tmpid   , c_dbcolname, c_desc    , c_path    ,c_parent_path,
                    d_ddltime , d_datadate , c_status  , c_sourceno)
                   select :p_sourceid||:p_separtor_for_id||to_char(a.OBJECT_ID)||:p_separtor_for_id||to_char(b.COLUMN_ID),
                          B.COLUMN_NAME,
                          D.COMMENTS,
                          c.USERNAME ||:p_separtor|| b.TABLE_NAME || :p_separtor|| B.COLUMN_NAME,
                          c.USERNAME ||:p_separtor|| b.TABLE_NAME,
                          a.LAST_DDL_TIME,
                          SYSDATE,
                          '0',
                          :p_sourceid
                     from user_objects      a,
                          user_tab_columns  b,
                          user_users        c,
                          USER_COL_COMMENTS D
                    where a.OBJECT_TYPE = 'TABLE'
                      and a.LAST_DDL_TIME > to_date(:his_datadate,'YYYYMMDD-hh24:mi:ss')
                      and a.OBJECT_NAME = b.TABLE_NAME
                      and B.TABLE_NAME = D.TABLE_NAME
                      and B.COLUMN_NAME = D.COLUMN_NAME'''
    #print(sql_text4)
    cr4=db.cursor()
    cr4.execute(sql_text4_1)
    cr4.execute(sql_text4_2,(param_sql_text4_2,param_sql_text4_4,param_sql_text4_4,param_sql_text4_3,param_sql_text4_3,param_sql_text4_3,param_sql_text4_2,param_sql_text4_1))
    db.commit()
    cr4.close()
    #将匹配的对象信息插入确认表中--关键字全匹配
    #C_STATUS=0状态为待落地
    param_sql_text5_1={'p_sourceid':",".join(sourceid)}
    sql_text5_1='''insert into SYSTEM_BI_OBJCONFIRM
                              (C_TMPID       ,C_OBJID   ,C_PREPID    ,C_OBJDESC,C_PATH   ,
                               C_PARENT_PATHB,C_OBJTYPE ,D_CONFIRM   ,C_STATUS ,D_DDLTIME,
                               c_dbcolname   ,C_SOURCENO,C_DICTITEMNO)
                       select a.c_tmpid,TO_CHAR(systimestamp,'YYYYMMDD-hh24missff3') C_OBJID,
                              rank()over(partition by a.c_path order by a.c_dbcolname) C_PREPID,
                              A.C_DESC,A.C_PATH,A.C_PARENT_PATH,B.C_OBJTYPE,trunc(sysdate,'dd'),
                             '0' C_STATUS,
                              a.d_ddltime,
                              a.c_dbcolname,a.c_sourceno,b.c_dictitemno
                        from SYSTEM_BI_TMPOBJS a
                        join SYSTEM_BI_OBJMATCHLIST b
                          on a.c_dbcolname = b.c_keyword
                       where b.c_type = '1' 
                         and a.c_status in ('0','3')
                         --and a.C_PATH<>b.C_PATH
                         --and a.c_sourceno=:p_sourceid 
                         '''
    cr5=db.cursor()

    cr5.execute(sql_text5_1,param_sql_text5_1)               
    #将临时表中状态为待确认的记录(包含历史对象)状态匹配的对象信息插入确认表中--关键字模糊匹配               
    sql_text5_2='''insert into SYSTEM_BI_OBJCONFIRM
                              (C_TMPID      ,C_OBJID   ,C_PREPID ,C_OBJDESC,C_PATH   ,
                               C_PARENT_PATH,C_OBJTYPE ,D_CONFIRM,C_STATUS ,D_DDLTIME,
                               c_dbcolname  ,C_SOURCENO,C_DICTITEMNO)
                       select a.c_tmpid,TO_CHAR(systimestamp,'YYYYMMDD-hh24missff3') C_OBJID,
                              rank()over(partition by a.c_path order by a.c_dbcolname) C_PREPID,
                              A.C_DESC,A.C_PATH,A.C_PARENT_PATH,B.C_OBJTYPE,
                              trunc(sysdate,'dd'),
                             '0' C_STATUS,
                              a.d_ddltime,
                              a.c_dbcolname,
                              a.c_sourceno,b.c_dictitemno
                        from SYSTEM_BI_TMPOBJS a
                        join SYSTEM_BI_OBJMATCHLIST b
                          on instr(a.c_dbcolname,b.c_keyword) >1
                       where b.c_type IN  ('2','3') 
                         and a.c_status in ('0','3')
                         --and a.C_PATH<>b.C_PATH
                         --and a.c_sourceno=:p_sourceid 
                         '''
    cr5.execute(sql_text5_2,param_sql_text5_1)                                             
    #更新临时表的记录状态,0-待确认,1-已插入,2-被排除,3-历史数据
    #将已插入确认标的 数据在临时表中
    sql_text5_4='''update SYSTEM_BI_TMPOBJS a set a.c_status='3' 
                        where exists (select 1 
                                        from SYSTEM_BI_OBJCONFIRM b 
                                       where a.c_tmpid=b.c_tmpid 
                                         and a.c_sourceno=b.c_sourceno)
                              and a.c_sourceno=:p_sourceid '''
    cr5.execute(sql_text5_4,param_sql_text5_1)                
    db.commit()
    cr5.close()
        
    #往对象信息主表SYSTEM_BI_OBJINFO插入数据
    param_sql_text6_1={'p_sourceid':",".join(sourceid)}
        #sql_text6_1='''insert into SYSTEM_BI_OBJINFO
        #                 (C_OBJID,C_OBJDESC,D_DDLTIME,C_SOURCENO)
        #               select C_OBJID,C_OBJDESC,D_DDLTIME,C_SOURCENO 
        #                 from SYSTEM_BI_OBJCONFIRM 
        #                where c_sourceno=:p_sourceid
        #                  and c_status='0'  '''
        #获取最大ID
    sql_text6_2='SELECT NVL(MAX(C_PREPID),0) FROM SYSTEM_BI_RELATEPREPINFO'
    cr6=db.cursor()
    cr5.execute(sql_text6_2) 
    MAX_PREPID=cr6.fetchone()
        
    #C_STATUS状态SYSTEM_BI_TMPOBJS的C_STATUS，0-已更新至预处理表;1-未更新至预处理表
    param_sql_text6_2={'p_max_prepid':",".join(MAX_PREPID)}
    sql_text6_3='''insert into SYSTEM_BI_RELATEPREPINFO
                              (C_OBJID     ,C_PREPID,C_PATH  ,C_PARENT_PATH,C_PREPDESC,
                               C_DICTITEMNO,C_TMPID ,C_STATUS,C_SOURCENO)
                       select b.C_OBJID,
                              :p_max_prepid+rank()over(partition by a.c_tmpid order by a.c_parent_path) C_PREPID,
                              a.C_PATH,
                              A.C_PARENT_PATH,
                              a.c_desc,
                              b.C_DICTITEMNO,
                              a.c_tmpid,
                              '1',
                              a.c_sourceno
                         from SYSTEM_BI_TMPOBJS a
                         join SYSTEM_BI_OBJCONFIRM b 
                           on b.c_status='0'
                          and a.c_status='3'
                          and a.c_sourceno=b.c_sourceno
                          and a.c_parent_path=b.c_parent_path
                          and a.c_tmpid  <>b.c_tmpid'''
    sql_text6_4='''update SYSTEM_BI_OBJCONFIRM 
                          set C_STATUS='1' 
                        where C_STATUS='0'
                          and C_SOURCENO=:p_sourceid '''
    #cr6.execute(sql_text6_1,param_sql_text6_1)
    cr6.execute(sql_text6_3,param_sql_text6_2,param_sql_text6_1)
    cr6.execute(sql_text6_4,param_sql_text6_1)
    db.commit()
    #将预处理表的状态置为4-已为属性插入
    sql_text6_5='''update SYSTEM_BI_TMPOBJS a
                          set c_status='4' 
                        where exists (select 1 from SYSTEM_BI_RELATEPREPINFO b 
                                       where a.c_tmpid=b.c_tmpid
                                         and a.c_sourceno=b.c_sourceno
                                         and a.c_status='0')
                          and  c_sourceno=:p_sourceid'''
    cr6.execute(sql_text6_5,param_sql_text6_1)
    sql_text6_6='''update SYSTEM_BI_RELATEPREPINFO
                          set c_status='0' 
                        where c_status='1'
                          and c_sourceno=:p_sourceid'''
    cr6.execute(sql_text6_6,param_sql_text6_1)
    db.commit()    
    cr6.close()
    #字段字典信息已录入完成,下一步插入/更新路径和目录信息
    db.close()
    print('Did it')