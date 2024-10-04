import pandas as pd
import pyarrow.parquet as pq
from time import time
from sqlalchemy import create_engine
import argparse
import os

parser = argparse.ArgumentParser(description='Ingest parquet data to Postgres')

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db_name = params.db_name
    table_name = params.table_name
    url = params.url
    
    file_name = 'output.parquet'
    
    os.system(f"wget {url} -O {file_name}")
    # Get the file
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')
    
    parquet_reader = pq.ParquetFile(file_name)
    df = parquet_reader.read_row_group(0).to_pandas()
    
    df.head(n=0).to_sql(name=table_name,con=engine,if_exists='replace')
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
            
            chunk.to_sql(name=table_name,con=engine,if_exists='append')
            
            t_end = time()
            print('inserted another chunk..., took %.3f second' %(t_end - t_start))
            
        # Afficher le reste de la division euclidienne, si c'est applicable
        if total_rows % chunk_size != 0:
            print(f"Nombre de lignes restantes: {total_rows % chunk_size}")
            print(f"Groupe de lignes {i} chargé correctement! ")

# user 
# password
# host
# port
# database name
# table name
# url of the files
        
if __name__ == '__main__':
    parser.add_argument('--user',  help='user name for postgres')
    parser.add_argument('--password',  help='password for postgres')
    parser.add_argument('--host',  help='host name for postgres')
    parser.add_argument('--port',  help='port for postgres')
    parser.add_argument('--db_name',  help='database name for postgres')
    parser.add_argument('--table_name',  help='table name where the results will be written ')
    parser.add_argument('--url',  help='url link to the file')
    
    args = parser.parse_args()
    main(args)
