__author__ = 'francisco'

stat_script_filepath='python/external/stat.py'
stat_script_whereto=''

username=''
password = ''

regionserver='192.168.111.214'
masterip='192.168.111.213'

#runs duration in seconds. Should be at least 15min plus 2min of ramp up
train_duration='1800'

perUpdate=0
perRead=1
perScan=0
scanDistribution='constant'

template= 'python/tmp/workloada_train_template'
target= 'python/tmp/workloada_train'

