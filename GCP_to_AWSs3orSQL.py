import json
import gspread
import awswrangler
import pandas as pd
import boto3 
import pymysql
import io


def lambda_handler(event, context):
    # 

    verificar={
        "type": "",
        "project_id": "",
        "private_key_id": "",
        "private_key": "",
        "client_email": "",
        "client_id": "",
        "auth_uri": "",
        "token_uri": "n",
        "auth_provider_x509_cert_url": "",
        "client_x509_cert_url": "
        }
    gc = gspread.service_account_from_dict(verificar)
    
    
    sh=gc.open('')
    wks=sh.worksheet("worksheet")
    datos=pd.DataFrame(wks.get_all_records())
    
    
    bucket = ''
    
    print('to_csv')
    csv_buffer = io.StringIO()
    datos.to_csv(csv_buffer, encoding='utf8', sep=',', index=False)
    key='.csv'
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
    
    
    
    
        try:
                conn = pymysql.connect(user="",password="", host="",database="")

        except:
                print("connection failed ")
        

        print(conn)
        cursor = conn.cursor()
        

        
        sql_Delete_query = f"""TRUNCATE TABLE _stg"""
        print(sql_Delete_query)
        cursor.execute(sql_Delete_query)
        conn.commit()

        
        """
        for a in data.columns:
                data[a]=np.where(data[a].notna()==False,0,data[a])
        """
        =.drop_duplicates(keep='first')
        cols = "`,`".join([str(i) for i in .columns.tolist()])

        #Insert DataFrame recrds one by one.
        
                #Insert DataFrame recrds one by one.
        
        for i,row in vw_dataview_click_marketing.iterrows():
                sql = "INSERT INTO `_stg` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"

                cursor.execute(sql, tuple(row))
                conn.commit()
                
        
        call_sp = f"""call """
        print(call_sp)
        cursor.execute(call_sp)
        conn.commit()
    print('ok all process')
