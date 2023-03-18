import pandas as pd

df = pd.read_json('Drake_transformed_releases.json', orient='columns', compression='infer')
print(df.head())