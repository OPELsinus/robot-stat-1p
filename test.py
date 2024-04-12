




























import pandas as pd

df = pd.read_excel(r'\\172.16.8.87\d\.rpa\Распредедение по бухгалтерам для стата.xlsx')

df['Филиал'] = df['Филиал'].apply(lambda x: x.replace(' ', '').replace('ПФ', 'ППФ'))

print(df)

df.to_excel(r'\\172.16.8.87\d\.rpa\Распредедение по бухгалтерам для стата1.xlsx', index=False)


