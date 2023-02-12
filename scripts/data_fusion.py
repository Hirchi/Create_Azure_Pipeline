# imports
import pandas as pd

# read csv files
df1 = pd.read_csv('AMAZON.csv')
df2 = pd.read_csv('APPLE.csv')
df3 = pd.read_csv('FACEBOOK.csv')
df4 = pd.read_csv('GOOGLE.csv')
df5 = pd.read_csv('MICROSOFT.csv')
df6 = pd.read_csv('ZOOM.csv')
df7 = pd.read_csv('TESLA.csv')

# merge csv files
df = pd.concat([df1, df2, df3, df4, df5, df6, df7], ignore_index=True)
print(df.shape)
# save to csv
df.to_csv('stocks.csv', index=False)

