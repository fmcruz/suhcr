#!/bin/bash

java -Djava.ext.dirs=lib/ -cp bin/ HBaseTuner $@ 
