#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine


# In[25]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')
parquet_reader = pq.ParquetFile('yellow_tripdata_2024-01.parquet')


# In[35]:


print(f"Le fichier yellow_tripdata_2024-01.parquet contient {parquet_reader.metadata.num_rows} lignes")
print(f"Ces lignes sont classées en {parquet_reader.num_row_groups} groupes")
taxi2024 = parquet_reader.read_row_group(0).slice(0, 1).to_pandas()


# In[ ]:


print(pd.io.sql.get_schema(taxi2024,name='yellow_taxi_jan2024',con=engine))


# In[32]:


# Nombre de lignes par groupe
chunk_size = 100000

# Nombre total de lignes
total_rows = parquet_reader.metadata.num_rows

num_groups = parquet_reader.num_row_groups

for i in range(num_groups):
    
    total_rows = parquet_reader.read_row_group(i).num_rows
    
    for start in range(0, total_rows, chunk_size):
        
        t_start = time()
        
        end = min(start + chunk_size, total_rows)  # Déterminer l'index de fin

        # Lire les lignes dans un DataFrame pandas
        chunk = parquet_reader.read_row_group(i).slice(start, end - start).to_pandas()

        # Traiter le chunk (par exemple, afficher les données)
        print(f"Chunk de lignes {start // chunk_size + 1}:")
        
        chunk.to_sql(name='yellow_taxi_jan2024',con=engine,if_exists='append')
        
        t_end = time()
        print('inserted another chunk..., took %.3f second' %(t_end - t_start))
        
    # Afficher le reste de la division euclidienne, si c'est applicable
    if total_rows % chunk_size != 0:
        print(f"Nombre de lignes restantes: {total_rows % chunk_size}")
        print(f"Groupe de lignes {i} chargé correctement! ")
    



# In[ ]:


from sqlalchemy import create_engine
engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[25]:


engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')


# In[ ]:


print(pd.io.sql.get_schema(taxi2024,name='yellow_taxi_Schema'))


# In[ ]:


print(pd.io.sql.get_schema(taxi2024,name='yellow_taxi_Schema',con=engine))


# In[44]:


taxi2024.head().to_sql(name='yellow_taxi_jan2024',con=engine,if_exists='replace')

