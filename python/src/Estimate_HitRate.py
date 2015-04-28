#!/usr/bin/python
import logging
import config.training_config as config
import os
import time
import paramiko
import math
import json
import che_test_hit_rate
from os import listdir
from os.path import isfile, join

__author__ = 'francisco'


class Estimate_HitRate(object):

    def __init__(self):
        self.localpath = os.path.dirname(os.path.abspath(__file__))
        print self.localpath
    
    def createDirs(self,tmp_dir):
        os.popen("mkdir -p "+self.localpath+"/"+tmp_dir)
        print "REMOVING DIR: "+"rm -rf "+self.localpath+"/"+tmp_dir+"/*"
        os.popen("rm -rf "+self.localpath+"/"+tmp_dir+"/*")

    def downloadTableHdfsFile(self,server,table,regionName,tmp_dir):
        x = os.popen("curl \"http://"+server+":50070/webhdfs/v1/"+table+"?op=LISTSTATUS\"").read()
        #print x
        js = json.loads(x)
        #print js
        i=0
        while (i < len(js['FileStatuses']['FileStatus'])):
            if(int(js['FileStatuses']['FileStatus'][i]['length'])>1002):
              path = js['FileStatuses']['FileStatus'][i]['pathSuffix']
              if (path.split("-")[3]==regionName):
                #print "wget -O tmp/"+table+"/"+path +" http://"+server+":50070/webhdfs/v1/"+table+"/"+path+"?op=OPEN"
                os.popen("wget -O "+self.localpath+"/"+tmp_dir+"/"+path +" http://"+server+":50070/webhdfs/v1/"+table+"/"+path+"?op=OPEN")
            #print js['FileStatuses']['FileStatus'][i]['pathSuffix']
            i+=1
        print "Table downloaded"

    def unmarshallToFreq(self,tmp_dir):
        path_to_jar = self.localpath.rsplit("/",2)[0]+"/java/MeT2.jar"
        freqs_dir = self.localpath+"/"+tmp_dir+"/freqs"
        os.popen("mkdir -p "+freqs_dir)
        os.popen("java -jar "+path_to_jar+" "+self.localpath+"/"+tmp_dir+"/")

        print "Unmarshalling finished"
    

    def estimate_HT(self,tmp_dir,region_access_probs,cache_size):
        x=[]
        #mypath = self.localpath+"/"+tmp_dir+"/freqs/"
        mypath = tmp_dir+"/freqs/"
        onlyfiles = [ f for f in listdir(mypath) if isfile(join(mypath,f)) ]
        numberOfRegions = len(onlyfiles)
        size = 0

        for path in onlyfiles:
            sum = 0
            
            if path != '.DS_Store':
                regionName = path.rsplit(".",2)[0].split("-")[3]
                file = open(mypath+path, 'rb')
                for line in file:
                    sum+=int(line)
                    size+=1
                #print sum
                
                file = open(mypath+path, 'rb')
                #print file
                probAccess = region_access_probs[regionName]
                print "probAccess: "+str(probAccess)
                for line in file:
                    x.append((int(line)/(sum+0.0)) * probAccess)

        print "Going to calculate Hit rate"
        ht = che_test_hit_rate.start(x,cache_size)
        file.close()
        print "ESTIMATED HIT RATE: "+str(ht)
        return ht,size
 
    def estimate_HT_uniform(self,size_key_space,cache_size):
        print "size_key_space: "+ str(size_key_space)
        probKey = 1.0 / size_key_space
        print "probKey: " +str(probKey)
        i=0
        x=[]
        while i < size_key_space:
                x.append(probKey)
                i+=1
        ht = che_test_hit_rate.start(x,cache_size)
        print "ESTIMATED HIT RATE UNIFORM: "+str(ht)
        return ht