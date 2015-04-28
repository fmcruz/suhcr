__author__ = 'francisco'

stat_script_filepath='/home/gsd/MeT2.0/python/external/stat.py'
stat_script_whereto='/home/gsd'

regionserver='192.168.111.215'
masterip='192.168.111.213'

#runs duration in seconds. Should be at least 15min plus 2min of ramp up
train_duration='1800'

perUpdate=0
perRead=1
perScan=0
scanDistribution='constant'

master = 'master'
template= '/home/gsd/MeT2.0/python/tmp/workloada_train_template'
target= '/home/gsd/MeT2.0/python/tmp/workloada_train'
whereto= '/opt/hbase-0.92.0-cdh4b1-rmv/conf/'
username=''
password = ''
