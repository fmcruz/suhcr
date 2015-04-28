"""Functions for modelling and evaluating the performance of cache
    replacement policies.
    FROM ICARUS PROJECT http://icarus-sim.github.io
    """
from __future__ import division
import math

import numpy as np
#from scipy.optimize import optimize
import scipy.optimize as optimize

from os import listdir
from os.path import isfile, join

__all__ = [
           'che_characteristic_time',
           'che_per_content_cache_hit_ratio',
           'che_cache_hit_ratio',
           'laoutaris_cache_hit_ratio',
           'optimal_cache_hit_ratio',
           'numeric_cache_hit_ratio'
           ]


def che_characteristic_time(pdf, cache_size, target=None):
    """Return the characteristic time of an item or of all items, as defined by
        Che et al.
        
        Parameters
        ----------
        pdf : array-like
        The probability density function of an item being requested
        cache_size : int
        The size of the cache (in number of items)
        target : int, optional
        The item index for which characteristic time is requested. If not
        specified, the function calculates the characteristic time of all the
        items in the population.
        
        Returns
        -------
        r : array of float or float
        If target is None, returns an array with the characteristic times of
        all items in the population. If a target is specified, then it returns
        the characteristic time of only the specified item.
        """
    def func_r(r, i):
        return sum(math.exp(-pdf[j]*r) for j in range(len(pdf)) if j != i) \
            - len(pdf) + 1 + cache_size
    items = range(len(pdf)) if target is None else [target]
    #print 'che_characteristic_time items: '+str(items)
    r = [optimize.fsolve(func_r, x0=50, args=(i)) for i in items]
    return r if target is None else r[0]


def che_per_content_cache_hit_ratio(pdf, cache_size, target=None):
    """Estimates the cache hit ratio of an item or of all items using the Che's
        approximation.
        
        Parameters
        ----------
        pdf : array-like
        The probability density function of an item being requested
        cache_size : int
        The size of the cache (in number of items)
        target : int, optional
        The item index for which cache hit ratio is requested. If not
        specified, the function calculates the cache hit ratio of all the items
        in the population.
        
        Returns
        -------
        cache_hit_ratio : array of float or float
        If target is None, returns an array with the cache hit ratios of all
        items in the population. If a target is specified, then it returns
        the cache hit ratio of only the specified item.
        """
    items = range(len(pdf)) if target is None else [target]
    r = che_characteristic_time(pdf, cache_size, 0)
    print "Calculated characteristic_time"
    print r
    
    #hit_ratio = [1 - math.exp(-pdf[i]*r[i]) for i in items]
    hit_ratio = [1 - math.exp(-pdf[i]*r[0]) for i in items]
    #for j in items:
    #print 'J: '+str(j)
    return hit_ratio if target is None else hit_ratio[0],r


def che_cache_hit_ratio(pdf, cache_size):
    """Estimates the overall cache hit ratio of an LRU cache under generic IRM
        demand using the Che's approximation.
        
        Parameters
        ----------
        pdf : array-like
        The probability density function of an item being requested
        cache_size : int
        The size of the cache (in number of items)
        
        Returns
        -------
        cache_hit_ratio : float
        The overall cache hit ratio
        """
    ch,r = che_per_content_cache_hit_ratio(pdf, cache_size)
    return sum(pdf[i]*ch[i] for i in range(len(pdf)))


def start(x,cache_size):
    return che_cache_hit_ratio(np.array(x),cache_size)
#print 'HIT RATE 2-LRU:' +str(che_cache_hit_ratio_two(np.array(x),cache,cache2))

