import os
import pandas as pd
# set admin ward code to NA if there isn't one
# Neo4J doesn't handle empty fields
df_out = pd.read_csv("./output/postcode_with_projections.csv")
df_out['Admin_ward_code'].fillna("NA", inplace = True) 
df_out.to_csv("./output/postcode_with_projections_filled_admin_ward_code.csv", index=None)