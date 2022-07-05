import pandas as pd
for chunk in pd.read_csv('2018Training_Data.csv', chunksize=10000):
    print(chunk)