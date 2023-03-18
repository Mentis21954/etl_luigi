import pandas as pd

df = pd.read_json('artist_contents.json', orient='index')
print(df['Content'][0])