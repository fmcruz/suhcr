#! /usr/bin/env python
from wsgiref.handlers import format_date_time
from time import *
import random
import string
import os
import re
from datetime import datetime
import sys
import csv

us=[]
sy=[]
ni=[]
id=[]
wa=[]
hi=[]
si=[]
st=[]

l1m=[]
l5m=[]
l15m=[]

def cpu():

  res = os.popen('top -b -d0.2 -n2 | grep Cpu').read()  

 # aux1 = re.split('\n',res)
#  print aux1
  aux = re.split(',',re.split('\n',res)[1])
  #print aux
#  if(time() > i):
#  	us.append(re.split('%',re.split(':',aux[0])[1])[0].strip())
#  	sy.append(re.split('%',aux[1])[0].strip())
#  	ni.append(re.split('%',aux[2])[0].strip())
  id.append(re.split('%',aux[3])[0].strip())
# 	wa.append(re.split('%',aux[4])[0].strip())
#  	hi.append(re.split('%',aux[5])[0].strip())
#  	si.append(re.split('%',aux[6])[0].strip())
#  	st.append(re.split('%',aux[7])[0].strip())


#  print "\n\nCPU\n"  
#  print "us " + str(us)
#  print "sy " + str(sy)
#  print "ni " + str(ni)
#  print "id " + str(id)
#  print "wa " + str(wa)
#  print "hi " + str(hi)
#  print "si " + str(si)
#  print "st " + str(st)

def load():
  
  res = os.popen('top -b -n1 | grep load').read()

 # aux1 = re.split('\n',res)
#  print aux1
  aux = re.split(',',re.split('average:',res)[1])

#  print aux[2]
  if(i>120):
  	l1m.append(aux[0].strip())
  	l5m.append(aux[1].strip())
  	l15m.append(aux[2].rstrip('\n').strip())

#  print "\n\n LOAD \n"
#  print "l1m " + str(l1m)
#  print "l5m " + str(l5m)
#  print "l15m " + str(l15m)


temporun=int(sys.argv[1])
i=1
inicio = int(time())
fim = inicio + temporun
i = inicio + 120


while(time()<fim):
 
 #print "Start CPU: %s" % ctime()
 cpu()
 #print "End CPU : %s" % ctime()
 #load()
# print "End load: %s" % ctime()
 #print "antes sleep"
 #sleep(0.25) 
 #print "End sleep: %s" % ctime()


fcpu = open('cpu.txt', 'w')
fcpu.write("us "+str(us)+"\n")
fcpu.write("sy "+str(sy)+"\n")
fcpu.write("ni "+str(ni)+"\n")
fcpu.write("id "+str(id)+"\n")
fcpu.write("wa "+str(wa)+"\n")
fcpu.write("hi "+str(hi)+"\n")
fcpu.write("si "+str(si)+"\n")
fcpu.write("st "+str(st)+"\n")
fcpu.close()

fload = open('load.txt', 'w')
fload.write("l1m "+ str(l1m)+"\n")
fload.write("l5m "+ str(l5m)+"\n")
fload.write("l15m "+ str(l15m)+"\n")
fload.close()


fcpu_user_CSV = open('cpu_user_CSV.txt', 'w')
wr = csv.writer(fcpu_user_CSV, quoting=csv.QUOTE_ALL)
wr.writerow(us)
fcpu_user_CSV.close()

fcpu_sy_CSV = open('cpu_sy_CSV.txt', 'w')
wr = csv.writer(fcpu_sy_CSV, quoting=csv.QUOTE_ALL)
wr.writerow(sy)
fcpu_sy_CSV.close()

fcpu_id_CSV = open('cpu_id_CSV.txt', 'wb')
#print str(id)
wr = csv.writer(fcpu_id_CSV, quoting=csv.QUOTE_ALL)
#wr = csv.writer(fcpu_id_CSV, quoting=csv.QUOTE_MINIMAL)
wr.writerow(id)
fcpu_id_CSV.close()

fcpu_id = open('cpu_id.txt', 'wb')
for element in id:
  fcpu_id.write(element+'\n')
fcpu_id.close()

fcpu_wa_CSV = open('cpu_wa_CSV.txt', 'w')
wr = csv.writer(fcpu_wa_CSV, quoting=csv.QUOTE_ALL)
wr.writerow(wa)
fcpu_wa_CSV.close()
