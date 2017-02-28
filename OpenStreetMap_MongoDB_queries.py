# -*- coding: utf-8 -*-
"""
Created on Tue Feb 28 10:01:42 2017

@author: D.Rudolf-Lvovsky
"""

from pymongo import MongoClient
import pprint
import numpy as np
from pymongo import GEOSPHERE
from bson.son import SON


client = MongoClient('localhost:27017')
db = client.OSM
# construct the geospatial index (2d sphere)
db.map_Duesseldorf_Neuss.create_index([("pos", GEOSPHERE)])
# some simple queries
size = db.map_Duesseldorf_Neuss.find().count()
print "The number of documents is {}.".format(size)
num_nodes = db.map_Duesseldorf_Neuss.find({"general_type": "node"}).count()
print "The number of nodes is {}.".format(num_nodes)
num_ways = db.map_Duesseldorf_Neuss.find({"general_type": "way"}).count()
print "The number of ways is {}.".format(num_ways)

# number of users
pipeline = [{'$group': { '_id': '$created.user',}}
            ]
result_list = []
result = db.map_Duesseldorf_Neuss.aggregate(pipeline)
while result.alive == True:
    result_list.append(result.next())
print 'The number of unique users is {}.'.format(len(result_list)) 

# most contributing users
pipeline = [{'$group': { '_id': '$created.user',
                              'count': {'$sum': 1 }  }},
            
                {'$sort': {'count': -1}}]
result = db.map_Duesseldorf_Neuss.aggregate(pipeline)
[result.next() for i in range(0,10)] 

pipeline = [{'$group': { '_id': '$created.user',
                              'count': {'$sum': 1 }  }},
             {'$project': {'ratio': {'$divide':['$count', 1e-2*size]}}},
            
                {'$sort': {'ratio': -1}}]
result = db.map_Duesseldorf_Neuss.aggregate(pipeline)
result_list = [result.next() for i in range(0,10)] 
pprint.pprint(result_list)

elem_sum = 0
for elem in result_list:
    elem_sum = elem_sum + elem['ratio']
print '\n Top 10 users contribute {0:.2f} % to the OSM for the Neuss/Duesseldorf area.'.format(elem_sum)     
 
# number of key = fixme or key = FIXME being close to our house (lat 51.22085, lon 6.65236)
# our house
coord = [6.65236,51.22085]
pipeline = [{'$geoNear':{ 'distanceField': 'pos','near': coord, 'spherical': True, 
                             'query': { 'fixme': {'$exists': 1}}, 'maxDistance': 1.5e-3, 
                             'distanceField': 'dist.calculated'}}
            ]
result_list = []
result = db.map_Duesseldorf_Neuss.aggregate(pipeline)
while result.alive == True:
    result_list.append(result.next())
print 'The number of dictionaries with key = fixme or key = FIXME is {}.'.format(len(result_list))            

# minimum and maximum longitude and latitude coordinates and total area
lat_max = 60.0
lat_min = 45.0
lon_max = 10.0
lon_min = 5.0

def get_pipeline(v_min, v_max, sort):

        min_datetime = '2016-01-01T00:00:00Z'
        version = '1'
        # gets values of pos between v_min and v_max with timestamp greater than min_datetime
        # and with version greater than 1
        # and with an address dict
        # sort = -1: descending, sort = 1: ascending
        pipeline = [{'$unwind': '$pos'},
                {'$match': {'$and':
                            [{'pos': {'$lt': v_max}}, {'pos': {'$gt': v_min}} ]}},
                {'$match': {'created.timestamp': {'$gt': min_datetime}}},
                {'$match': {'created.version': {'$gt': version}, 'address': {'$exists': 1} }},   
                {'$project': {'pos':'$pos', 'address':'$address'}},
                {'$sort': {'pos': sort}}
                ]
        return pipeline
    
def get_min_max_coord(v_min, v_max):
    
    for sort in [-1,1]:
        result_list = []
        pipeline = get_pipeline(v_min, v_max,sort)
        result = db.map_Duesseldorf_Neuss.aggregate(pipeline, allowDiskUse = True )
        result_list = [result.next() for i in range(0,1)]
       # pprint.pprint(result_list) 
        
        if sort == -1:
            print 'The max value {0:.5f}.'.format(result_list[0]['pos'])
        else:
            print 'The min value {0:.5f}.'.format(result_list[0]['pos'])

get_min_max_coord(lat_min, lat_max)
get_min_max_coord(lon_min, lon_max)


def get_distance(lat_1, lat_2, lon_1, lon_2):
# all in km and radians   
    coords = np.pi/180.0*np.array([lat_1, lat_2, lon_1, lon_2])
    R = 6371.0
    
    return 2.0*R*np.arcsin(np.sqrt( np.sin(0.5*(coords[1]-coords[0]))**2 + np.cos(coords[0])*np.cos(coords[1])
                                   *np.sin(0.5*(coords[3]-coords[2]))**2 )) 
# five decimal places = 1 m accuracy
d_south_north = get_distance(51.16696,51.23432,6.64838,6.64838)
d_west_east = get_distance(51.23432,51.23432,6.64838,6.87130)
area = d_south_north*d_west_east
print 'The south-north distance is {0:.3f} km.'.format(d_south_north)
print 'The west-east distance is {0:.3f} km.'.format(d_west_east)
print 'The total area is {0:.3f} km^2.'.format(area) 


# number of pharmacies on the map
pipeline = [{'$group': { '_id': '$amenity',
                              'count': {'$sum': 1 }  }},
             {'$match': {'_id': 'pharmacy'}},
            
                {'$sort': {'count': -1}}
           ]
result = db.map_Duesseldorf_Neuss.aggregate(pipeline)
result_list = []
while result.alive == True:
    result_list.append(result.next()) 

print 'The number of pharmacies on the map is {}.'.format(result_list[0]['count'])
print 'The average number of pharmacies per km^2 is {}.'.format(result_list[0]['count']/area)

# number of amenities with key = wheelchair
pipeline = [{'$group': { '_id': '$amenity',
                              'count': {'$sum': 1 }  }},
             {'$match': {'_id': 'pharmacy'}},
            
                {'$sort': {'count': -1}}
           ]
result = db.map_Duesseldorf_Neuss.aggregate(pipeline)
result_list = []
while result.alive == True:
    result_list.append(result.next()) 

print 'The number of pharmacies on the map is {}.'.format(result_list[0]['count'])
print 'The average number of pharmacies per km^2 is {}.'.format(result_list[0]['count']/area)


# top ten of amenities with a wheelchair access
pipeline = [{'$match': { 'wheelchair': 'yes', 'amenity':{'$exists':1}}},
            {'$group': { '_id': '$amenity',
                       'count': {'$sum': 1 }}},
            {'$sort': {'count': -1}}
           ]
result = db.map_Duesseldorf_Neuss.aggregate(pipeline)
result_list = []
while result.alive == True:
    result_list.append(result.next()) 

print 'Top ten of amenities with a wheelchair access.'     
pprint.pprint(result_list[0:10]) 
