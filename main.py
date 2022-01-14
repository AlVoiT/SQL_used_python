from clickhouse_driver import connect as cc_connect
from clickhouse_driver import Client


from gspread_dataframe import get_as_dataframe, set_with_dataframe

from oauth2client.service_account import ServiceAccountCredentials
import gspread

import time
import pandas as pd

import logging


logfile = 'log_coldcalls.log'

log = logging.getLogger("my_log")
log.setLevel(logging.INFO)
FH = logging.FileHandler(logfile)
basic_formater = logging.Formatter('%(asctime)s : [%(levelname)s] : %(message)s')
FH.setFormatter(basic_formater)
log.addHandler(FH)


client = Client(host='localhost', user='***', password='***')

FILE_NAME = 'coldcalls-335107-35a8ff6110ea.json'

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(FILE_NAME , scope)
clientggl = gspread.authorize(creds)

sh = clientggl.open("coldcalls")
worksheet_Result = sh.worksheet("Result")
worksheet_Filter = sh.worksheet("Filter")
worksheet_Reference = sh.worksheet("Reference")
worksheet_Update = sh.worksheet("Update")  
worksheet_Insert = sh.worksheet("Insert")  


def ch_get_df(sql):
    cc_conn = cc_connect(host='localhost', user='***', password='***', database='***')
    with cc_conn:
        df = pd.read_sql(sql, cc_conn)
    return df

def filter_clear(sheet):
    colm ,data = sheet.batch_get(('A7:AA7','A8:AA'))
    colm = colm[0]
    data = data + [[1 for i in range(len(colm))]]
    df = pd.DataFrame(data, columns = colm)
    df.drop(df.tail(1).index, inplace=True) # drop last row
    df.replace("", float("NaN"), inplace=True)
    df = df.dropna(axis=1, how='all')
    filter_dict = df.to_dict('list')
    filter_dict = {key: [i for i in values if i] for key, values in filter_dict.items()}
    list_filters = [f'{key} in {values}' for key, values in filter_dict.items()]
    filters = ' and '.join(list_filters)
    if filters:
        filters = 'where ' + filters 
        
    return filters

def sqlquery(filrt, result_sheet, off, lim):    
    string_filter = f"""
    select * 
    from ovoitenko.test
    {filter_clear(filrt)} 
    limit {off}, {lim}
    """
    result = ch_get_df(string_filter)
    set_with_dataframe(result_sheet, result)
    
def filter_update(sheet):
    colm ,data = sheet.batch_get(('A2:AA2','A3:AA3'))
    colm = colm[0]
    data = data + [[1 for i in range(len(colm))]]
    df = pd.DataFrame(data, columns = colm)
    df.drop(df.tail(1).index, inplace=True) # drop last row
    df.replace("", float("NaN"), inplace=True)
    df = df.dropna(axis=1, how='all')
    filter_dict = df.to_dict('list')
    filter_dict = {key: [i for i in values if i] for key, values in filter_dict.items()}
    list_filters = [f'{key} in {values}' for key, values in filter_dict.items()]
    filters_up = ' and '.join(list_filters)
    if filters_up:
        filters_up = 'where ' + filters_up
    else:
        sheet.update_cell(3, 1, "Updated all data")
        
    return filters_up 

def data_update(sheet):
    colm ,data = sheet.batch_get(('A7:AA7','A8:AA'))
    colm = colm[0]
    data = data + [[1 for i in range(len(colm))]]
    df = pd.DataFrame(data, columns = colm)
    df.drop(df.tail(1).index, inplace=True) # drop last row
    df.replace("", float("NaN"), inplace=True)
    df = df.dropna(axis=1, how='all')
    data_list = df.to_dict('records')
    select = ['phone', 'mobile_phone']
#   Если фильтр телефон, то значение будет в виде списка, иначе строка
    list_data = [f"{key} = {values}" if key in select  else f"{key} = '{values}'"  for dicts in data_list for key, values in dicts.items()]
    data_up = ', '.join(list_data)
        
    return data_up

def data_insert(sheet):
    colm ,data = sheet.batch_get(('A2:AA2','A3:AA'))
    colm = colm[0]
    data = data + [[1 for i in range(len(colm))]]
    df = pd.DataFrame(data, columns = colm)
    df['phone'] = '[' + df['phone'].astype(str) + ' ]'
    df['mobile_phone'] = '[' + df['mobile_phone'].astype(str) + ' ]'
    df.drop(df.tail(1).index, inplace=True) # drop last row
    df.replace("", float("NaN"), inplace=True)
    df = df.dropna(axis=1, how='all')
    columns = str(tuple(df.columns)).replace("'", "")
    list_str = [str(tuple(df.iloc[i].fillna(0))).replace("'[", "['").replace("]'", "']") for i in range(len(df.index))]
    list_values_str = [f'{columns} VALUES  {value}' for value in list_str ]    
    
    return list_values_str

def sql_insert(sheet):
    for insert in data_insert(sheet):
        sql = f"""INSERT INTO ovoitenko.test 
        {insert}"""
        client.execute(sql)

def sql_update(sheet):
    sql = f"""
    ALTER TABLE ovoitenko.test 
    UPDATE {data_update(sheet)} 
    {filter_update(sheet)}"""
    try:
        client.execute(sql)
    except Exception as e:
        try:
            log.error(str(e)[:str(e).find('Stack trace:')])
        except Exception:
            log.error(e)
        finally:
            log.info("Don't update data")
            sheet.update('B3', "ERROR!!! Edit query and try again")

def main(filrt, result, update, insert):
    filrt.update('B2', " ")
    
    while True:

        START = filrt.acell('B1').value
        UPDATE = update.acell('B1').value
        INSERT = insert.acell('B1').value

        if START=='1':
            OFFSET = filrt.acell('C4').value
            LIMIT = filrt.acell('B4').value
            result.clear()
            sqlquery(filrt=filrt, result_sheet=result, off=OFFSET, lim=LIMIT)
            filrt.update('B1', 0)
        elif UPDATE=='1':
            sql_update(update)
            update.update('B1', 0)
        elif INSERT=='1':
            sql_insert(insert)
            insert.update('B1', 0)
        else:
            pass
            
        time.sleep(10)
#вывод еррор в текстовый файл, и основыне ошибки отдебажить        
if __name__ == "__main__":
    log.info("START")
    try:
        main(filrt=worksheet_Filter, result=worksheet_Result, update=worksheet_Update ,insert=worksheet_Insert)
        log.info("END")
    except Exception as e:
        try:
            log.error(str(e)[:str(e).find('Stack trace:')])
        except Exception:
            log.error(e)
        finally:
            worksheet_Filter.update('B2', "ERROR!!!")
            log.info("END with ERROR")
