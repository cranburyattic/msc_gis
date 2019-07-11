from py2neo import Graph
import numpy as np
import pandas as pd

graph = Graph("bolt://localhost:7687", auth=("neo4j", "test"))

df_500 = graph.run(
  """MATCH (l:Line)-[:IS_ON]-(s)<-[:IS_WITHIN]-(:Postcode)-[:SALE_IN]-(sl)  
  WITH l, count(sl) as totalSales ORDER BY count(sl) DESC LIMIT 5
  MATCH (l)<-[i:IS_ON]-(s)<-[:IS_WITHIN {distance : 500}]-(p)<-[t:SALE_IN]-(sl)
  WITH DISTINCT(sl) as sl,s,l 
  WITH  [x IN collect(sl) WHERE x.year = 1998 | x.price] as sales1998,[x IN collect(sl) WHERE x.year = 2008 | x.price] as sales2008,[x IN collect(sl) WHERE x.year = 2018 | x.price] as sales2018,s,l
  RETURN l.name as lineName, s.name as stationName, s.location.x as longitude, s.location.y as latitude, COALESCE(apoc.coll.avg(sales1998),0) as avg1998_500, COALESCE(apoc.coll.avg(sales2008),0) as avg2008_500,COALESCE(apoc.coll.avg(sales2018),0) as avg2018_500"""
).to_data_frame()

df_1000 = graph.run(
  """MATCH (l:Line)-[:IS_ON]-(s)<-[:IS_WITHIN]-(:Postcode)-[:SALE_IN]-(sl)  
  WITH l, count(sl) as totalSales ORDER BY count(sl) DESC LIMIT 5
  MATCH (l)<-[i:IS_ON]-(s)<-[:IS_WITHIN {distance : 1000}]-(p)<-[t:SALE_IN]-(sl)
  WITH DISTINCT(sl) as sl,s,l 
  WITH  [x IN collect(sl) WHERE x.year = 1998 | x.price] as sales1998,[x IN collect(sl) WHERE x.year = 2008 | x.price] as sales2008,[x IN collect(sl) WHERE x.year = 2018 | x.price] as sales2018,s,l
  RETURN l.name as lineName, s.name as stationName, s.location.x as longitude, s.location.y as latitude, COALESCE(apoc.coll.avg(sales1998),0) as avg1998_1000, COALESCE(apoc.coll.avg(sales2008),0) as avg2008_1000,COALESCE(apoc.coll.avg(sales2018),0) as avg2018_1000"""
).to_data_frame()


#df = graph.run(
#  """MATCH (l:Line)<-[i:IS_ON]-(s)<-[:IS_WITHIN {distance : 500}]-(p)<-[t:SALE_IN]-(sl) WHERE (s.distanceFromCentre > 10500 AND s.distanceFromCentre < 11500) OR (s.distanceFromCentre > 4500 AND s.distanceFromCentre < 5500)  OR (s.distanceFromCentre > 14500 AND s.distanceFromCentre < 15500)   OR (s.distanceFromCentre > 19500 AND s.distanceFromCentre < 20500)
#  WITH DISTINCT(sl) as sl,s,l
#  WITH  [x IN collect(sl) WHERE x.year = 1998 | x.price] as sales1998,[x IN collect(sl) WHERE x.year = 2008 | x.price] as sales2008,[x IN collect(sl) WHERE x.year = 2018 | x.price] as sales2018,s,l
#  RETURN l.name as lineName, s.name as stationName, s.location.x as longitude, s.location.y as latitude, COALESCE(apoc.coll.avg(sales1998),0) as avg1998, COALESCE(apoc.coll.avg(sales2008),0) as avg2008,COALESCE(apoc.coll.avg(sales2018),0) as avg2018"""
#  ).to_data_frame()

df_500['avg1998_500'] = df_500['avg1998_500'].astype(np.int64)
df_500['avg2008_500'] = df_500['avg2008_500'].astype(np.int64)
df_500['avg2018_500'] = df_500['avg2018_500'].astype(np.int64)

df_1000['avg1998_1000'] = df_1000['avg1998_1000'].astype(np.int64)
df_1000['avg2008_1000'] = df_1000['avg2008_1000'].astype(np.int64)
df_1000['avg2018_1000'] = df_1000['avg2018_1000'].astype(np.int64)

#def change1998to2018(x):
#  if(x['avg1998'] == 0 or x['avg2018'] == 0) :
#    return 0
#  else :
#    return (x['avg2018'] - x['avg1998'])/x['avg1998'] * 100

def change1998to2018(x,suffix):
  return changeBetweenYears(x,'avg1998' + suffix,'avg2018' + suffix)
def change1998to2008(x,suffix):
  return changeBetweenYears(x,'avg1998' + suffix,'avg2008' + suffix)
def change2008to2018(x,suffix):
  return changeBetweenYears(x,'avg2008' + suffix,'avg2018' + suffix)

def changeBetweenYears(x,year1, year2) :
  if(x[year1] == 0 or x[year2] == 0) :
    return 0
  else :
    return (x[year2] - x[year1])/x[year1] * 100

df_500["change1998to2018_500"] = df_500.apply(lambda x: change1998to2018(x,'_500'), axis=1)
df_500["change1998to2008_500"] = df_500.apply(lambda x: change1998to2008(x,'_500'), axis=1)
df_500["change2008to2018_500"] = df_500.apply(lambda x: change2008to2018(x,'_500'), axis=1)


df_1000["change1998to2018_1000"] = df_1000.apply(lambda x: change1998to2018(x,'_1000'), axis=1)
df_1000["change1998to2008_1000"] = df_1000.apply(lambda x: change1998to2008(x,'_1000'), axis=1)
df_1000["change2008to2018_1000"] = df_1000.apply(lambda x: change2008to2018(x,'_1000'), axis=1)


df_500.to_csv('~/test_500.csv')
df_1000.to_csv('~/test_1000.csv')

df_combined = pd.merge(df_500, df_1000, on=['lineName', 'stationName'])


def test(x) : 
  if(x['change1998to2018_500'] > x['change1998to2018_1000']) :
    return 'U'
  elif(x['change1998to2018_500'] < x['change1998to2018_1000']) :
    return 'D'
  else:
    return 'N'

df_combined["change"] = df_combined.apply(lambda x: test(x), axis=1)
df_combined.to_csv('~/test_500_1000.csv')