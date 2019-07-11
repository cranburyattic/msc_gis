import os
import pandas as pd
import pyproj
from pyproj import Proj, transform

df_out = pd.read_csv("./output/postcode.csv", header=0)


inProj = Proj(init='epsg:27700')
outProj = Proj(init='epsg:4326')

def get_longitude(x) :
  x, y = transform(inProj,outProj,x.Eastings,x.Northings)
  return x

def get_latitude(x) :
  x, y = transform(inProj,outProj,x.Eastings,x.Northings)
  return y

df_out['longitude'] = df_out.apply(lambda x: get_longitude(x), axis=1)
df_out['latitude'] = df_out.apply(lambda x: get_latitude(x), axis=1)

print(df_out)
df_out.to_csv("./output/postcode_with_projections.csv", index=None)

