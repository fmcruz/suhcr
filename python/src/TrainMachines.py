#!/usr/bin/python
import logging
import config.training_config as config
import os
import time
import paramiko
import MeTGlue
import math
__author__ = 'francisco'

class TrainMachinesMeT(object):

    def __init__(self):
        self._metGlue =  MeTGlue.MeTGlue()
        logging.info('Connected to MeTGlue gateway.')
        self.rserver=config.regionserver
        self.master=config.masterip
        self.stat_script_filepath = config.stat_script_filepath
        self.stat_script_whereto = config.stat_script_whereto
        self.train_duration = config.train_duration
        self.rootPassword=config.password
        self.username=config.username

        self.localpath = os.path.dirname(os.path.abspath(__file__))

        self.perRead = config.perRead
        self.perUpdate = config.perUpdate
        self.perScan = config.perScan
        self.scanDistribution = config.scanDistribution

        self._sizeCounter = [1000000,2000000,3000000,4000000,6000000,8000000,12000000]
        self._targets = [95.0,80.0,60.0,40.0,20.0]
        self._tolerance = 0.25
        
        self._udpatestargets = [5,10,25,50,75,100,250,500,750,1000,1500,2000,2500,3000,3500,4000,4500,5000]
        
        self._scanstargets = [500,750,825,1000,1500,2000,3000,4000,4625]
        self._scansLength  = [1,2,5,10,20,30,40,50,60,70,80,90,100,110,120,130,150]

        #number of runs for each point
        self._times = 3
        #dictionary containing the results of previous runs for the current cpu_target
        self._pastValues = {}
        #dictionary containing the results of the training
        self._resultsDic = {}


    def trainMachine(self,target,tolerance,inithrough,times,C95result,min_value,max_value):
        LOW = inithrough #training_config.initialThroughput
        HIGH = inithrough #training_config.initialThroughput
        Current = inithrough #training_config.initialThroughput
        H_perc = 0
        L_perc = 0
        C_perc = 0
        counter = 0
        Current_prev = Current


        cpu_target = 100-target
        low_interval = (100-target)-tolerance
        high_interval = (100-target)+tolerance
        
        #main loop on/off
        stop = True
        
        C_perc, whatTime  = self.runMultipleTimes(Current,times)
        if(whatTime != -1):
 		self._pastValues[C_perc] = Current
        else:
		if(C_perc < cpu_target):
                        C_perc = cpu_target + 1

        counter += 1
        
        logging.info('LOW:'+str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))

        if (LOW / HIGH) == 1 and (C_perc<=cpu_target):

            if (C_perc >= low_interval):
                stop=False
                logging.info('FINAL0. COUNTER: '+str(counter)+' LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+str(Current)+ ';C_perc:'+ str(C_perc))    
            else:    
                LOW = Current
                #Current = int(Current * 2)
                logging.info('C95result: '+str(C95result))
                if(C95result==-1):
                    Current_prev = Current
                    Current = Current * 2
                    if (Current > max_value) and (max_value != -1): 
                        Current_prev = Current
			Current = max_value
                else:
                    Current_prev = Current
                    Current = Current + int(((cpu_target - C_perc)/5)*C95result)
                    if(Current > max_value) and (max_value != -1): 
                        Current_prev = Current
			Current = max_value
                HIGH = Current
                H_perc = C_perc
                L_perc = C_perc
                C_perc, whatTime = self.runMultipleTimes(Current,times)
                counter += 1
            while stop:
                if (whatTime == -1):
                    #    if(Current > Current_prev):
				if(C_perc < cpu_target):
					C_perc = cpu_target + 1
                
                time.sleep(5)
                logging.info('LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))
                if (C_perc >= low_interval) and (C_perc <= high_interval) and (whatTime != -1):
                    stop = False
                    logging.info('FINAL1. COUNTER: '+str(counter)+' LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+str(Current)+ ';C_perc:'+ str(C_perc))     
                elif (HIGH != LOW) and (L_perc <= cpu_target) and (H_perc > cpu_target) and (C_perc > cpu_target):
                    H_perc = C_perc
                    HIGH = Current
                    Current_prev = Current
                    Current = int((Current + LOW ) / 2.0)
                    if(Current_prev == Current):
                        stop = False
                        logging.info('FINAL2. COUNTER: '+str(counter)+' LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+str(Current)+ ';C_perc:'+ str(C_perc))
                    else:
                        logging.info('2 if'+'LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))
                        C_perc, whatTime = self.runMultipleTimes(Current,times)
                        counter += 1
                elif (HIGH != LOW) and (L_perc <= cpu_target) and (H_perc > cpu_target) and (C_perc <= cpu_target):
                    LOW = Current
                    L_perc = C_perc
                    Current_prev = Current
                    Current = int((Current + HIGH ) / 2.0)
                    if(Current_prev == Current):
                        stop = False
                        logging.info('FINAL3. COUNTER: '+str(counter)+' LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+str(Current)+ ';C_perc:'+ str(C_perc))
                    else:
                        logging.info('3 if'+'LOW:'+str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))
                        C_perc, whatTime = self.runMultipleTimes(Current,times)
                        counter += 1
                elif (HIGH != LOW) and (L_perc <= cpu_target) and (H_perc <= cpu_target) and (C_perc <= cpu_target):
                    LOW = HIGH
                    L_perc = C_perc
                    if(C95result==-1):
			Current_prev = Current
                        Current = Current * 2
                        if(Current > max_value) and (max_value != -1): 
                            Current_prev = Current
                            Current = max_value
                    else:
			Current_prev = Current
                        Current = Current + int(((cpu_target - C_perc)/5)*C95result)
                        if(Current > max_value) and (max_value != -1):
			    Current_prev = Current 
                            Current = max_value
                    HIGH = Current
                    H_perc = C_perc
                    logging.info('4 if'+'LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))
                    C_perc, whatTime = self.runMultipleTimes(Current,times)
                    counter += 1
                elif (HIGH != LOW) and (L_perc <= cpu_target) and (H_perc <= cpu_target) and (C_perc > cpu_target):
                    H_perc = C_perc
                    Current_prev = Current
                    Current = int((HIGH + LOW ) / 2.0)
                    logging.info('5 if'+'LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))
                    C_perc, whatTime = self.runMultipleTimes(Current,times)
                    counter += 1
                elif (HIGH == LOW):
                    #safeguard
                    if (C_perc > cpu_target):                    
                        if(C95result==-1):
                            Current_prev = Current
                            Current = int(Current / 2.0)
                            if(Current < min_value):
                                Current_prev = Current 
                                Current = min_value
                        else:
                            Current_next = Current - int(((C_perc-cpu_target)/5)*C95result)
                            if (Current_next <= 0):
                                Current_prev = Current
                                Current = int(Current / 2.0)
                                if(Current < min_value):
                                    Current_prev = Current 
                                    Current = min_value
                            else:
				Current_prev = Current
                                Current = Current - int(((C_perc-cpu_target)/5)*C95result) 
                                if(Current < min_value):
                                    Current_prev = Current 
                                    Current = min_value
                        LOW = Current
                        H_perc = C_perc
                        L_perc = C_perc
                        logging.info('6 if'+'LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))
                        C_perc, whatTime = self.runMultipleTimes(Current,times)    
                        counter+=1
                    else:
                        LOW = HIGH
                        L_perc = C_perc
                        if(C95result==-1):
                            Current_prev = Current
                            Current = Current * 2
                            if(Current > max_value) and (max_value != -1): 
                                Current_prev = Current
                                Current = max_value
                        else:
                            Current_prev = Current
                            Current = Current + int(((cpu_target - C_perc)/5)*C95result)
                            if(Current > max_value) and (max_value != -1):
                                Current_prev = Current 
                                Current = max_value
                        HIGH = Current
                        H_perc = C_perc
                        logging.info('7 if'+'LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))
                        C_perc, whatTime = self.runMultipleTimes(Current,times)
                        counter += 1    
        else:
            if (C_perc <= high_interval):
                stop=False
                logging.info('FINAL0.1. COUNTER: '+str(counter)+' LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+str(Current)+ ';C_perc:'+ str(C_perc))   
            else:
                HIGH = Current
                logging.info('C95result: '+str(C95result))
                if(C95result==-1):
                    Current_prev = Current
                    Current = int(Current / 2.0) 
                    if(Current < min_value): 
                        Current_prev = Current
                        Current = min_value                   
                else:
                    Current_next = Current - int(((C_perc-cpu_target)/5)*C95result)
                    if (Current_next <= 0):
                        Current_prev = Current
                        Current = int(Current / 2.0)
                        if(Current < min_value):
                            Current_prev = Current 
                            Current = min_value 
                    else:
                        Current_prev = Current
                        Current = Current - int(((C_perc-cpu_target)/5)*C95result) 
                        if(Current < min_value):
                            Current_prev = Current 
                            Current = min_value      
                LOW = Current
                H_perc = C_perc
                L_perc = C_perc
                C_perc, whatTime = self.runMultipleTimes(Current,times)
                counter += 1
            while stop:
                time.sleep(5)
                logging.info('LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))
                if (C_perc >= low_interval) and (C_perc <= high_interval) and (whatTime != -1):
                    stop = False
                    logging.info('FINAL4. COUNTER: '+str(counter)+' LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+str(Current)+ ';C_perc:'+ str(C_perc))                                   

                elif (HIGH != LOW) and (L_perc <= cpu_target) and (H_perc > cpu_target) and (C_perc > cpu_target):
                    HIGH = Current
                    H_perc = C_perc
                    Current_prev = Current
                    Current = int((Current + LOW) / 2.0)
                    if(Current_prev == Current):
                        stop = False
                        logging.info('FINAL5. COUNTER: '+str(counter)+' LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+str(Current)+ ';C_perc:'+ str(C_perc))
                    else:
                        logging.info('8 if'+'LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))
                        C_perc, whatTime = self.runMultipleTimes(Current,times)
                        counter += 1

                elif (HIGH != LOW) and (L_perc <= cpu_target) and (H_perc > cpu_target) and (C_perc <= cpu_target):
                    LOW = Current
                    L_perc = C_perc
                    Current_prev = Current
                    Current = int((Current + HIGH) / 2.0)
                    if(Current_prev == Current):
                        stop = False
                        logging.info('FINAL6. COUNTER: '+str(counter)+' LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+str(Current)+ ';C_perc:'+ str(C_perc))
                    else:
                        logging.info('9 if'+'LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))
                        C_perc, whatTime = self.runMultipleTimes(Current,times)
                        counter += 1

                elif (HIGH != LOW) and (L_perc > cpu_target) and (H_perc > cpu_target) and (C_perc > cpu_target):
                    HIGH = Current
                    if(C95result==-1):
                        Current_prev = Current
                        Current = int(Current / 2.0)
                        if(Current < min_value):
                            Current_prev = Current 
                            Current = min_value 
                    else:
                        Current_next = Current - int(((C_perc-cpu_target)/5)*C95result)
                        if (Current_next <= 0):
                            Current_prev = Current
                            Current = int(Current / 2.0)
                            if(Current < min_value):
                                Current_prev = Current 
                                Current = min_value 
                        else:
                            Current_prev = Current
                            Current = Current - int(((C_perc-cpu_target)/5)*C95result) 
                            if(Current < min_value):
                                Current_prev = Current 
                                Current = min_value 
                    LOW = Current
                    H_perc = C_perc
                    L_perc = C_perc
                    logging.info('10 if'+'LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))
                    C_perc, whatTime = self.runMultipleTimes(Current,times)
                    counter += 1
                elif (HIGH != LOW) and (L_perc > cpu_target) and (H_perc > cpu_target) and (C_perc <= cpu_target):
                    Current_prev = Current
                    Current = int((HIGH + LOW ) / 2.0)
                    L_perc = C_perc
                    logging.info('11 if'+'LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))
                    C_perc, whatTime = self.runMultipleTimes(Current,times)
                    counter += 1
                elif (HIGH == LOW):
                    #safeguard
                    if (C_perc > cpu_target):                    
                        if(C95result==-1):
                            Current_prev = Current
                            Current = int(Current / 2.0)
                            if(Current < min_value):
                                Current_prev = Current 
                                Current = min_value 
                        else:
                            Current_next = Current - int(((C_perc-cpu_target)/5)*C95result)
                            if (Current_next <= 0):
                                Current_prev = Current
                                Current = int(Current / 2.0)
                                if(Current < min_value):
                                    Current_prev = Current 
                                    Current = min_value 
                            else:
                                Current_prev = Current
                                Current = Current - int(((C_perc-cpu_target)/5)*C95result)  
                                if(Current < min_value):
                                    Current_prev = Current 
                                    Current = min_value   
                        LOW = Current
                        H_perc = C_perc
                        L_perc = C_perc
                        logging.info('12 if'+'LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))
                        C_perc, whatTime = self.runMultipleTimes(Current,times)    
                        counter+=1
                    else:
                        LOW = HIGH
                        L_perc = C_perc
                        #Current = Current * 2
                        if(C95result==-1):
                            Current_prev = Current
                            Current = Current * 2
                            if(Current > max_value) and (max_value != -1):
                                Current_prev = Current 
                                Current = max_value
                        else:
                            Current_prev = Current
                            Current = Current + int(((cpu_target - C_perc)/5)*C95result)
                            if(Current > max_value) and (max_value != -1): 
                                Current_prev = Current
                                Current = max_value
                        HIGH = Current
                        H_perc = C_perc
                        logging.info('13 if'+'LOW:'+ str(LOW)+ ';HIGH:'+ str(HIGH)+ ';L_perc:'+ str(L_perc)+ ';H_perc:'+ str(H_perc)+ ';Current:'+ str(Current)+ ';C_perc:'+ str(C_perc))
                        C_perc, whatTime = self.runMultipleTimes(Current,times)
                        counter += 1
                           
        return Current        



    def runMultipleTimes(self,Current,times,scanlength = None):
        table = "workloadA"

        result = -1
        pastValuesR = {}
        pastValuesR.update(reversed(i) for i in self._pastValues.items())
        whatTime = -2

        if Current in pastValuesR:
            result = pastValuesR[Current]
            logging.info('Current: '+str(Current)+' already tested in previous run, using stored value!!')
        else:            
            i = 0
            absolute_average = 0.0
            while i < times:
                self.restartServer(self.rserver)
                while(not self.isTableOnline(self.rserver,table)):
                    logging.info('Tables not yet online after RS restart sleeping more 15s.')
                    time.sleep(15)  
                while(self._metGlue.getStorefiles(table)):
                     self._metGlue.majorCompact(table)
                     while(self.isBusyCompacting(self.rserver)):
                        logging.info('Waiting for major compact to finish in '+str(self.rserver)+'...')
                        time.sleep(20)
		             self.restartServer(self.rserver)
                     while(not self.isTableOnline(self.rserver,table)):
                        logging.info('Tables not yet online after RS restart sleeping more 15s.')
                        time.sleep(15)
                self.cleanCaches(self.rserver)

                averageCpuidle,whatTime =  self.run(Current)
                stdDev = self.std_Deviation(averageCpuidle,whatTime)
                absolute_average += averageCpuidle            
                i+=1
                if(stdDev <= 10) and i==1: break

            result = 100-(absolute_average/i)
            if(whatTime != -1):
            	self._pastValues[result] = Current
        return result, whatTime

    def run(self,Current,scanlength = None):
        #heat up RS cache
	    command = ("/home/"+self.username+"/YCSB/bin/ycsb run hbase -s -threads 75 -p columnfamily=ycsb -p table=workloadA -p maxexecutiontime=900 -p exportfile=a_3.txt -P /home/"+self.username+"/YCSB/workloads/workloada_train -p readproportion=1 -p updateproportion=0 -p requestdistribution=uniform -target 0 2>log_training.txt")
        logging.info(command)
        logging.info((os.system(command)))

        #RUN CPU STAT SCRIPT ON RS MACHINE
        self.runStatScript()        

        if scanlength is not None:
          command = ("/home/"+self.username+"/YCSB/bin/ycsb run hbase -s -threads 75 -p columnfamily=ycsb -p table=workloadA -p maxexecutiontime="+self.train_duration+" -p exportfile=a_3.txt -P /home/"+self.username+"/YCSB/workloads/workloada_train -p readproportion=" + str(self.perRead) + " -p updateproportion=" + str(self.perUpdate)+ " -p scanproportion=" + str(self.perScan)+ " -p scanlengthdistribution="+ str(self.scanDistribution)+" -p requestdistribution=uniform -target " + str(Current) +" 2>log_training.txt")
        else:
	      command = ("/home/"+self.username+"/YCSB/bin/ycsb run hbase -s -threads 75 -p columnfamily=ycsb -p table=workloadA -p maxexecutiontime="+self.train_duration+" -p exportfile=a_3.txt -P /home/"+self.username+"/YCSB/workloads/workloada_train -p readproportion=" + str(self.perRead) + " -p updateproportion=" + str(self.perUpdate)+" -p requestdistribution=uniform -target " + str(Current) +" 2>log_training.txt")
	    logging.info(command)
        logging.info((os.system(command)))
        logging.info(('ycsb ran.'))

        #AFTER YCSB HAS RUN COPY THE STAT FILE TO LOCALHOST
        #FIRST MAKE SURE THE SCRIPT IS FINISHED
        self.checkStatScriptFinished()
        self.copyStatScriptResultFile()
  
        fileLog = open('log_training.txt', 'rb')
        whatTime = -1
        currentHigh=Current*1.05
        currentLow=Current*0.95
        counter=0
        ops_sum=0
        average_ops=0
        for line in fileLog:
            strSplit = line.split(";")
            if(len(strSplit)>=3):
              if(len(strSplit[2])>2):
                ops=float(strSplit[1].split(" ")[1])
                if(int(strSplit[0].split(" ")[1])>150):
                 if(ops>=currentLow and ops<=currentHigh):
                     if(whatTime==-1):
                        whatTime = int(strSplit[0].split(" ")[1])
                        counter+=1
                        ops_sum+=ops
                 if(counter > 0):
                     counter+=1
                     ops_sum+=ops
        if(whatTime!=-1 and counter<11):
                whatTime=-1
        if(whatTime!=-1):
                average_ops = (ops_sum+0.0)/counter
                if(average_ops<currentLow):
                        whatTime=-1
        logging.info('Operations stabilize from: '+str(whatTime)+ " s.")

        file_len = self.file_len(self.localpath+'/cpu_id.txt')
        ## just account for the last half of the file
	
        correction_factor = (int(file_len)+0.0)/int(self.train_duration)
        whatTime = int(whatTime*correction_factor)

        file = open(self.localpath+'/cpu_id.txt', 'rb')
        sum = 0
        counter = 0
        counter_true = 0
        for line in file:
            if(counter>=whatTime):
            	sum += float(line)
                counter_true +=1
            counter += 1
        file.close()    
        averageCpuidle = (sum + 0.0)/counter_true   
        logging.info('averageCpuidle: '+str(averageCpuidle))

        return averageCpuidle, whatTime 
   
    def file_len(self,fname):
     with open(fname) as f:
        for i, l in enumerate(f):
            pass
     return i + 1 

    def std_Deviation(self,mean,whatTime):
        file_len = self.file_len(self.localpath+'/cpu_id.txt')
        file = open(self.localpath+'/cpu_id.txt', 'rb')
        sum = 0
        counter = 0
        counter_true = 0
        for line in file:
	        if(counter>=whatTime):
            	sum += (float(line)-mean)**2
                counter_true +=1
            counter += 1            
        file.close()    
        stdDev = math.sqrt((sum+0.0)/counter_true)
        logging.info('std_Deviation_Cpuidle: '+str(stdDev))

        return stdDev
    
    # returns the throughput for the closest cpu_target
    def findClosestKey(self,target):
        if target in self._pastValues:
            return target
        else:
            return min(self._pastValues.keys(), key=lambda k: abs(k-target))  


    def loadData(self,recordcount):        
        table="workloadA"
        cf="ycsb"
        os.system("sed 's/SIZE/"+str(recordcount)+"/g' /home/"+self.username+"/MeT2.0/python/tmp/workloada_train_template.txt > /home/"+self.username+"/YCSB/workloads/workloada_train")
        
        if(self._metGlue.tableExists(table)):   
            logging.info("Table exists; removing table")         
            self._metGlue.removeTable(table)
        
        self._metGlue.createTable(table,cf)    
                
        command = ("/home/"+self.username+"/YCSB/bin/ycsb load hbase -p columnfamily="+cf+" -p table="+table+" -P /home/"+self.username+"/YCSB/workloads/workloada_train -threads 100 -s 2> YCSBload.txt")
        logging.info(command)
        logging.info((os.system(command)))
        logging.info(('ycsb loaded'))
        self.restartServer(self.rserver)

        try:
            logging.info('Major compact of:'+table) 
            while(not self.isTableOnline(self.rserver,table)):
                logging.info('Table:'+table+' not yet online after RS restart sleeping more 5s.')
                time.sleep(5)                    
            self._metGlue.majorCompact(table)
            while(self.isBusyCompacting(self.rserver)):
                logging.info('Waiting for major compact to finish in '+str(self.rserver)+'...')
                time.sleep(20)
            self.cleanCaches(self.rserver)
        except Exception, err:
                logging.error('ERROR:'+str(err))


    def runStatScript(self):
        logging.info("running StatScript on RS:"+self.stat_script_whereto+'/stat.py '+self.train_duration+' &') 

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
                ssh.connect(self.rserver, username=+self.username, password=self.rootPassword)
        except:
                logging.info("Unable to connect to node  " + str(self.rserver))    
        stdin, stdout, stderr = ssh.exec_command('chmod +x'+self.stat_script_whereto+'/stat.py')            
        stdin, stdout, stderr = ssh.exec_command(self.stat_script_whereto+'/stat.py '+self.train_duration+' &')

        ssh.close()
        logging.info('StatScript running in RS')


    def checkStatScriptFinished(self):    
        logging.info("Checking StatScript is finished on RS "+self.stat_script_whereto+'/stat.py') 

        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
                ssh.connect(self.rserver, username=self.username, password=self.rootPassword)
        except:
                logging.info("Unable to connect to node  " + str(self.rserver))    

        while(True):       
            stdin, stdout, stderr = ssh.exec_command('ps aux | grep stat.py')            
            running = False
            result = stdout.readlines()
            for ele in result:
                if ('python' in ele) and ('stat.py' in ele):                    
                    running = running or True 
            if not running: break                           
            logging.info('StatScript not finished yet, sleeping for 20s')        
            time.sleep(20)     
               
        ssh.close()

    def copyStatScriptToRS(self):
        logging.info('Copy Stat Script to regionserver')
        self.copyToServer(self.rserver,self.stat_script_whereto,self.stat_script_filepath)  

    def copyStatScriptResultFile(self):
        logging.info('Copy StatScript result')
        self.copyFromServer(self.rserver,self.localpath,'/home/'+self.username+'/cpu_id.txt')

    def copyToServer(self,host,whereto,filepath):
        logging.info("Copying files to "+ str(host))
        tries=0
        while tries<10:
            try:
                tries+=1
                private_key = paramiko.RSAKey.from_private_key_file("/home/"+self.username+"/.ssh/id_rsa")
                transport = paramiko.Transport((host, 22))
                transport.connect(pkey=private_key,username=self.username)
                transport.open_channel("session", host, "localhost")
                sftp = paramiko.SFTPClient.from_transport(transport)
                splittedpath = filepath.split('/')[-1]
                logging.info(filepath+ " "+whereto+'/'+splittedpath)
                sftp.put(filepath, whereto+'/'+splittedpath)
                sftp.close()
                transport.close()
                logging.info('File '+str(filepath)+' copied to '+str(host)+'.')
                break
            except IOError as e:
                logging.info("I/O error({0}): {1}".format(e.errno, e.strerror))
                logging.info("Unable to connect to node  " + str(host)+ " after "+str(tries)+" attempts.")
                time.sleep(5)               

    def copyFromServer(self,host,localpath,filepath):
        logging.info("Copying file from "+ str(host))
        tries=0
        while tries<10:
            try:
                tries+=1
                private_key = paramiko.RSAKey.from_private_key_file("/home/"+self.username+"/.ssh/id_rsa")
                transport = paramiko.Transport((host, 22))
                transport.connect(pkey=private_key,username=self.username)
                transport.open_channel("session", host, "localhost")
                sftp = paramiko.SFTPClient.from_transport(transport)
                splittedpath = filepath.split('/')[-1]
                logging.info(filepath+ " "+localpath+'/'+splittedpath)
                sftp.get(filepath,localpath+'/'+splittedpath)
                sftp.close()
                transport.close()
                logging.info('File '+str(filepath)+' copied to '+localpath+'/'+splittedpath+'.')
                break
            except IOError as e:
                logging.info("I/O error({0}): {1}".format(e.errno, e.strerror))
                #logging.info("Unable to connect to node  " + str(host)+ " after "+str(tries)+" attempts.")
                time.sleep(5) 


    def isTableOnline(self,server,table):
            x = os.popen("curl \"http://"+server+":60030/rs-status\"").read()
            return table in x

    def isBusyCompacting(self,server):
            x = os.popen("curl \"http://"+server+":60030/rs-status\"").read()
            return "RUNNING" in x
   
    def getHitCacheRatio(self,server,table):
             x = os.popen("curl \"http://"+server+":60030/rs-status?filter=general#regionHitStats\"").readlines()
             counter=0
             counterHit=-1
             hitRatio=-1
             switchHit=False
             workloadTable = table+","
             while counter<len(x):
                #logging.info(x[counter])
                if("class=\"tab-pane\" id=\"tab_regionHitStats\"" in x[counter]):
                    switchHit=True
                if(switchHit):
                   if(workloadTable in x[counter]):
                      counterHit=0
                   if(counterHit!=-1):
                      counterHit+=1
                   #   logging.info(x[counter])
                      if(counterHit==5):
                          logging.info("Hit cache ratio: "+str(x[counter]))
                          hitRatio=int(x[counter].split(">")[1].split("<")[0])
                          break
                counter+=1
             return hitRatio 
             
 
    def cleanCaches(self,host):
        logging.info("CLEAN CACHES")
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
                ssh.connect(host, username=self.username, password=self.rootPassword)
        except: 
                logging.info("Unable to connect to node  " + str(host))
        stdin, stdout, stderr = ssh.exec_command('echo'+self.rootPassword+ '| sudo -S  sysctl -w vm.drop_caches=3')
        logging.info(str(stdout.readlines()))
    

    def restartServer(self,host):
        logging.info("RESTART SERVER")
        host = self.master
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
                ssh.connect(host, username=self.username, password=self.rootPassword)
        except:
                logging.info("Unable to connect to node  " + str(host))
        stdin, stdout, stderr = ssh.exec_command('/home/'+self.username+'/hbase-0.98.3/bin/stop-hbase.sh')
        logging.info(str(stdout.readlines()))
        time.sleep(90)
        stdin, stdout, stderr = ssh.exec_command('/home/'+self.username+'/hbase-0.98.3/bin/start-hbase.sh')
        logging.info(str(stdout.readlines()))
        ssh.close()
        logging.info('Server '+str(host)+' restarted ('+str(stdout)+').')

    def addToresultsDic(self,size,cpu_target,throughput):
        if not self._resultsDic.has_key(size):
            self._resultsDic[size] = {}
        self._resultsDic[size][cpu_target] = throughput


    def start(self):
        self.copyStatScriptToRS()   
        readModel = False
        scanModel = True

        if(readModel):
            fileResult = open('result-training.txt', 'wb')
            fileResult.write('SIZE; TARGET; THROUGHPUT; HIT RATIO\n')
            fileResult.close()
            
            last95result = 102
            
            logging.info(self.getHitCacheRatio(self.rserver,"workloadA"))
            
            i=0;
            for size in self._sizeCounter:
                self.loadData(size)
                throughput_initial = last95result
                C95result = -1
                lastThroughput = -1
                self._pastValues = {}
                max_value = -1
                for cpu_target in self._targets:
                    if self._pastValues:
                        closestTarget = self.findClosestKey(100-cpu_target)
                        logging.info('closestTarget:'+str(closestTarget))
                        logging.info('closestTarget_Value:'+str(self._pastValues[closestTarget]))
                        if(closestTarget > cpu_target):
                            throughput_initial = self._pastValues[closestTarget] - int(((closestTarget - (100-cpu_target))/5)*C95result)
                        else:
                            throughput_initial = self._pastValues[closestTarget] + int((((100-cpu_target) - closestTarget)/5)*C95result)
                        self.cleanCaches(self.rserver)
                    if i>0:
                        #multiply the max_value by 10% just to account for some variance
                        max_value = int(self._resultsDic[self._sizeCounter[i-1]][cpu_target] * 1.1)
#                    self.cleanCaches(self.rserver)                    
                    throughput = self.trainMachine(cpu_target,self._tolerance,throughput_initial,self._times,C95result,lastThroughput,max_value)
                    if C95result == -1:
                        C95result = throughput
                        last95result = throughput
                    logging.info('C95result:'+str(C95result))
                    
                    lastThroughput = throughput
                    self.addToresultsDic(size,cpu_target,throughput)
                    fileResult = open('result-training.txt', 'a')
                    fileResult.write(str(size)+'; '+str(cpu_target)+'; '+str(throughput)+'; '+str(self.getHitCacheRatio(self.rserver,"workloadA"))+'\n')
                    fileResult.close()
                i+=1
        elif (scanModel):
            fileResult = open('result-training-scans.txt', 'wb')
            fileResult.write('SIZE; TARGET THROUGHPUT; SCAN LENGTH; CPU \n')
            fileResult.close()
        
            for size in self._sizeCounter:
              self.loadData(size)
                for target in self._scanstargets:
                    for scanlength in self._scansLength:
                      cpu_result, whatTime = self.runMultipleTimes(target,self._times,scanlength)
                      fileResult = open('result-training-scans.txt', 'a')
                      fileResult.write(str(size)+'; '+str(target)+'; '+str(scanlength)+'; '+str(cpu_result)+'\n')
                      fileResult.close()

        else:
            fileResult = open('result-training-updates.txt', 'wb')
            fileResult.write('SIZE; TARGET THROUGHPUT; CPU \n')
            fileResult.close()

            for size in self._sizeCounter:
                self.loadData(size)
                for target in self._udpatestargets:
                    cpu_result, whatTime = self.runMultipleTimes(target,self._times)
                    fileResult = open('result-training-updates.txt', 'a')
                    fileResult.write(str(size)+'; '+str(target)+'; '+str(cpu_result)+'\n')
                    fileResult.close()




