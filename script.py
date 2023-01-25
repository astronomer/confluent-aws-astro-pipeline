import pandas as pd

my_dict = {"Invoice":489434,"Description":"15CM CHRISTMAS GLASS BALL 20 LIGHTS","Customer ID":"13085","Price":6.95,"Quantity":12,"Country":"United Kingdom","InvoiceDate":"12/1/2009 07:45","Distribution ID":1,"StockCode":"85048"}

print(list(my_dict.values()))

print(list(my_dict.keys()))
print({k:[v] for k,v in my_dict.items()})
df = pd.DataFrame({k:[v] for k,v in my_dict.items()})

print(df)

