#! /usr/bin/env python

import string
import os
import sys

#from numpy import *
#import scipy as sp
#from pandas import *
#sudo pip install rpy2==2.3.8
from rpy2.robjects.packages import importr
from rpy2.robjects import IntVector
from rpy2.robjects import StrVector
import rpy2.robjects as ro
#import pandas.rpy.common as com


def estimate_cpu_reads(keySize,ops_observed):
    stats = importr('stats')

    #2921,2065,1276,562,118

    SIZE   = IntVector([100,100,95,90,80,70,60,49.9,39.9,33.3,24.96,16.64])
    REQ_80 = IntVector([33276,33276,16674,12692,8411,6371,3972,2921,1052,411,219,146])
    REQ_60 = IntVector([14724,14724,8356,6588,4636,3737,3065,2065,852,372,211,140])
    REQ_40 = IntVector([9632,9632,5068,4035,2898,2316,1912,1276,690,332,196,127])
    REQ_20 = IntVector([4672,4672,2517,1825,1295,1064,882,562,387,213,129,81])
    REQ_5  = IntVector([750,750,455,341,276,212,185,118,88,44,27,17])

    TYPE = StrVector(["monoH.FC"])

    ro.globalenv["SIZE"] = SIZE
    ro.globalenv["REQ_80"] = REQ_80
    ro.globalenv["REQ_60"] = REQ_60
    ro.globalenv["REQ_40"] = REQ_40
    ro.globalenv["REQ_20"] = REQ_20
    ro.globalenv["REQ_5"]  = REQ_5
    ro.globalenv["TYPE"]   = TYPE

    splinefun = ro.r['splinefun']
    sp_80 = splinefun(SIZE,REQ_80,TYPE)
    sp_60 = splinefun(SIZE,REQ_60,TYPE)
    sp_40 = splinefun(SIZE,REQ_40,TYPE)
    sp_20 = splinefun(SIZE,REQ_20,TYPE)
    sp_5  = splinefun(SIZE,REQ_5,TYPE)

    sps = [sp_5,sp_20,sp_40,sp_60,sp_80]
    cpu = [0.0,5.0,20.0,40.0,60.0,80.0]

    res = int(float(sp_80(keySize).r_repr()))
    i = 0
    x_sup = 0
    x_inf = 0
    res = 0
    res_prev=0

    while(i<5):
        res = int(float(sps[i](keySize).r_repr()))
        if(ops_observed >= res_prev and ops_observed<res):
            i=6
        else:
            res_prev=res
            x_inf+=1
        i+=1
        x_sup+=1



    if(x_sup==x_inf):
        cpu1_estimated=-1
    else:
        percentage_cpu = (ops_observed+0.0-res_prev)/(res-res_prev)
        cpu1_estimated = cpu[x_inf]+((cpu[x_sup]-cpu[x_inf])*percentage_cpu)


    return cpu1_estimated

def estimate_cpu_writes(ops_observed):
    stats = importr('stats')

    ops = IntVector([5,10,25,50,75,100,250,500,750,1000,1500,2000,2500,3000,3500,4000,4500,5000])
    cpu_used = IntVector([1.5733,1.5944,2.09,2.346,2.488,2.596,4.925,6.956,9.02,10.75,16.06,20.74,25.2100192678,30.2011560694,35.0838150289,38.0040540541,42.8837545126,47.6525096525])
    TYPE = StrVector(["monoH.FC"])
   
    ro.globalenv["ops"] = ops
    ro.globalenv["cpu_used"] = cpu_used
    ro.globalenv["TYPE"]   = TYPE

    splinefun = ro.r['splinefun']
    sp_w = splinefun(ops,cpu_used,TYPE)

    res = float(sp_w(ops_observed).r_repr())
    print '\ncpu_writes: ' + str(res)
    return res



