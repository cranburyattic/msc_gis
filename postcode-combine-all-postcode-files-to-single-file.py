import os
import pandas as pd

df_out = pd.read_csv("code-point-column-headers.csv", header=None, skiprows=1)

for file in os.listdir("./postcode_2"):
    if file.endswith(".csv"):
      df = pd.read_csv("./postcode_2/" + file, header=None)
      df_out = df_out.append(df, sort=False)
df_out.to_csv("./output/postcode.csv", index=None,header=None)