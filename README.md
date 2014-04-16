udf-trailmix
============

Aerospike UDF mixes

Aerospike is a higher performance cluster aware key-value datastore. It supports User Defined Function (UDF) which is code written in Lua programming language and runs inside the Aerospike database server. UDFs can be dynamically loaded into the Aerospike Cluster to created extended capability.

udf-trailmix contains lua examples. Enjoy !!!


Setup
=====
1. Download and Install : http://www.aerospike.com/free-aerospike-3-community-edition/
2. Start Aerospike      : sudo /etc/init.d/aerospike start


Aerospike UDF Documentation
===========================
https://docs.aerospike.com/display/V3/Lua+UDF+Guide

Quick Usage
===========
Download .lua files to the local directory. Run aql (https://docs.aerospike.com/pages/viewpage.action?pageId=3807532)

example: 

- aql> Register Module './redis.lua'
- aql> Execute redis.LPUSH('tweets', 'my simple tweet') on test.demo where PK = '1'
- aql> Execute redis.LPOP("tweets", 10) 



