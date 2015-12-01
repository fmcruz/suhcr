# suhcr
Python scripts to create a CPU model of HBase and Cassandra NoSQL database from the cache hit ratio.

It is composed by two modules:
A java module to interface with HBase, which is used by the pyhton script.
And a python module

The java module is run by:
./runner.sh HmasterIP zookeeperPort

The python module is run by:
./runner.sh

The Pyhton module has a config file:
python/src/config/training_config.py
Where necessary configurations to create the CPU model are.
