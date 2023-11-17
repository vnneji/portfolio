####### This Program downloads cryptocurrency rates from Lirium periodically and saves locally to a CSV, then uploads the CSV to a database
####### Execute with grep -v '^%' "lirium_rates_import.py" | python3

def lirium_rates_import(path, engine):
    
    ###Required modules:
    ###    import requests
    ###    from sqlalchemy import create_engine
    ###    import pandas as pd
    ###    import numpy as np
    ###    import csv
    ###    from datetime import datetime
    ###    import time
    ###    from sqlalchemy import text
    

    while True:

        try:
            now = datetime.now().strftime("%m_%d_%Y_%H_%M_%S")
            rundate = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

            csvfile = path+"lirium_rates_prod_"+now+".csv"

            url = "https://api.lirium.com/v1/prices"
            response = requests.get(url)

            df = pd.read_json(response.text)
            with open(csvfile, 'w') as f:
                pout = "{},{},{},{},{},{}\n".format('trade_ccy_code','ccy_name', 'exch_ccy_code', 'ask_bid', 'rate', 'rundate')  
                f.write(pout)
                df_index = df.index
                for i in df_index:
                    df_name = df.loc[i].quote_currencies['name']
                ##    if np.isnan(df.loc[i].prices):
                ##            break
                    try:
                        df_keys = df.loc[i].prices.keys()
                        for j in df_keys: 
                            ask_bid  = df.loc[i].prices[j].keys()
                            for k in ask_bid:
                                l = df.loc[i].prices[j][k]
                                pout = "{},{},{},{},{},{}\n".format(i,df_name, j, k, l, rundate)  
                                f.write(pout)
                    except:
                        continue


            df = pd.read_csv(csvfile)

            truncate_query = text("TRUNCATE TABLE bi_analytics.stg_lirium_rates")
            connection = engine.connect()
            connection.execution_options(autocommit=True).execute(truncate_query)
            connection.close()


            df.to_sql('stg_lirium_rates', schema = 'bi_analytics', con=engine, index=False, index_label='id', if_exists='replace')
            delete_query = text("delete from bi_analytics.lirium_rates where rundate = to_timestamp('"+rundate+"','dd/MM/yyyy hh24:mi:ss')")
            insert_query = text("insert into bi_analytics.lirium_rates select trade_ccy_code,ccy_name,exch_ccy_code,ask_bid,rate, to_timestamp(rundate, 'dd/mm/yyyy hh24:mi:ss') from bi_analytics.stg_lirium_rates")

            connection = engine.connect()
            connection.execution_options(autocommit=True).execute(delete_query)
            connection.commit()
            connection.close()

            connection = engine.connect()
            connection.execution_options(autocommit=True).execute(insert_query)
            connection.commit()
            connection.close()
            print("Lirium rates imported for "+rundate)

            delta = timedelta(hours=1)
            delta_minutes = timedelta(minutes=10)
            now = datetime.now()
            next_hour = (now + delta).replace(microsecond=0, second=0, minute=0)
            next_minute = (now + delta_minutes)

            wait_seconds = (next_hour - now).seconds
            wait_seconds_minutes = (next_minute - now).seconds

            time.sleep(wait_seconds_minutes)
        except Exception as e:
            print("Error: ", e)



        
#####FUNCTION CALL########
import requests
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import csv
from datetime import datetime
from datetime import timedelta
import time
from sqlalchemy import text

path = ##"Path To Save CSV Files" 


###Create SQL Engine using SQL Alchemy
%load_ext sql
%sql dbtype://username:password@hostname/dbname
engine = create_engine('dbtype://username:password@hostname/dbname')

lirium_rates_import(path, engine)
