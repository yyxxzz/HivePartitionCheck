# -*- coding: utf-8 -*-
from snakebite.minicluster import MiniCluster
from snakebite.client import Client
import pprint

#Hive数据仓库路径
Hive_Warehouse='/data/yoho/'
client = Client('yhbd01',8020,use_trash=False)


#由Hive库的路径,获取DB的路径
def get_DB(Hive_Warehouse):
    DB={}
    DB_re=[]
    client = Client('yhbd01',8020,use_trash=False)
    list_hive = list(client.ls([Hive_Warehouse]))
    for x in list_hive:
        DB=x
        DB_re.append(DB['path'])
    return DB_re

#由DB的路径,表的路径
def get_table(list_DB):
    TB={} #定义一个字典，将返回的结果赋值给这个字典，并从这个字典中取值
    TB_re=[]
    list_tab = list(client.ls([list_DB]))
    for x in list_tab:
        TB=x
        TB_re.append(TB['path'])
    return TB_re
 
#由表的路径,找到所有存在的分区
def get_part(list_table):
    PT={}
    PT_re=[]
    list_par = list(client.ls([list_table]))
    for x in list_par:
        PT=x
        PT_re.append(PT['path'])
    return PT_re

#给定参数,获取所有分区位置
def run():
    Hive_Warehouse='/data/yoho'
    list_DB=[]
    list_PA=[]
    list_status=[]
    list_DB = get_DB(Hive_Warehouse)
    for i in list_DB:
        #pprint.pprint(get_table(i)) 
        for o in get_table(i):
            #pprint.pprint(get_part(o))
            list_PA.append(get_part(o))
            #pprint.pprint(list(client.count([get_part(o)]))    
    return list_PA



#输出length,fileCount,spaceConsumed为零的分区路径，并发送报警邮件。
#['length', 'fileCount', 'directoryCount', 'quota', 'spaceConsumed', 'spaceQuota']
def get_part_status(part_list):
    ST={}
    ST_re=[]
    list_status = list(client.count([part_list]))
    for i in list_status:
        ST=i
        if ST['length'] == 0:
            ST_re.append(ST['length'])
    return ST_re


if __name__ == '__main__':
    #path1='/data/yoho/yoho_passport/user_vip/date_id=20150428'
    #path2='/data/yoho/user/idl_user_attr_day/20150826'
    pprint.pprint(run())
    

    



