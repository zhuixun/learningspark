__author__ = '49236_000'

import numpy
import os
import sys
from pyspark import SparkContext
import matplotlib.pyplot as hist


#u'1|24|M|technician|85711'
sc=SparkContext("local","Chapter2")
user_data=sc.textFile("file:///home/hadoop/data/ml-100k/u.user")
user_data.first()
user_fields=user_data.map(lambda line: line.split("|"))
num_user=user_fields.map(lambda fields: fields[0]).count()
num_genders=user_fields.map(lambda fields: fields[2]).distinct().count()
num_occupations=user_fields.map(lambda fields: fields[3]).distinct().count()
num_zipcodes=user_fields.map(lambda  fields: fields[4]).distinct().count()

print "User: %d,genders %d,occupations: %d,ZIP codes: %d" % (num_user,num_genders,num_occupations,num_zipcodes)


#histogram
ages = user_fields.map(lambda x: int(x[1])).collect()
hist(ages, bins=20, color='lightblue', normed=True)
fig = matplotlib.pyplot.gcf()
fig.set_size_inches(16, 10)

