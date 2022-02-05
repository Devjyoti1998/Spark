# -*- coding: utf-8 -*-
"""
Created on Sat Feb  5 20:43:24 2022

@author: user
"""

from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("AmountbyCustomer")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/Section1/customer-orders.csv")
pair = input.map(lambda x:(int(x.split(',')[0]),float(x.split(',')[2])))
amt=pair.reduceByKey(lambda x,y:x+y)
amt1=amt.sortByKey()
amt2=amt.sortBy(lambda x:x[1])

result=amt2.collect()

for res in result:
    print ('{}:\t{:.2f}'.format(res[0],res[1]))


print(type(result))
