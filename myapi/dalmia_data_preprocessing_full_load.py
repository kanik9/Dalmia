import sqlalchemy
import urllib
import datetime as dt
import pandas as pd
import json
import pyodbc
import logging

pd.set_option('display.max_rows', 100000000)
pd.set_option('display.max_columns', 100000000)
pd.set_option('display.width', 1000)


def vc_model_full_load(vc_model_raw, engine, plant_name):
    try:
        config_all = {'2.Clk-Input': {'header_list': [8, 9], 'format_list': '%b-%y', 'ctype': 'many', 'append': [0, 7]},
                      '3.Cem-Input': {'header_list': [10, 11], 'format_list': '%b-%y', 'ctype': 'many', 'append': [0, 11]},
                      '4.VC Summary': {'header_list': [1], 'format_list': "%b'%y", 'ctype': 'single', 'append': []},
                      '5.1.VC-RGP': {'header_list': [0, 1], 'format_list': "%b'%y", 'ctype': 'many', 'append': []},
                      '6.LS Cost': {'header_list': [2], 'format_list': '%b-%y', 'ctype': 'single', 'append': []},
                      '7.Power Mix': {'header_list': [3], 'format_list': '%b-%y', 'ctype': 'single', 'append': []},
                      '8.Slag': {'header_list': [2], 'format_list': '%b-%y', 'ctype': 'single', 'append': []},
                      '9.Flyash': {'header_list': [2], 'format_list': '%b-%y', 'ctype': 'single', 'append': []},
                      '9.1.Pond Ash': {'header_list': [2], 'format_list': '%b-%y', 'ctype': 'single', 'append': []},
                      '10.Coal_Petcoke': {'header_list': [2], 'format_list': '%b-%y', 'ctype': 'single', 'append': []},
                      '10.1.Coal_Petcoke Rate': {'header_list': [2], 'format_list': '%b-%y', 'ctype': 'single', 'append': []},
                      '11.GF': {'header_list': [3], 'format_list': '%b-%y', 'ctype': 'single', 'append': []},
                      '11.1.GF_RGP': {'header_list': [2], 'format_list': '%b-%y', 'ctype': 'single', 'append': []},
                      '12.Stock': {'header_list': [0, 2], 'format_list': '%b-%y', 'ctype': 'many', 'append': []}, }

        for name, config in config_all.items():
            print(name)
            df = pd.read_excel(vc_model_raw, sheet_name=name,
                               engine='pyxlsb', header=config['header_list'])
            df.columns = [dateparse1(
                x, config['format_list'], config['ctype']) for x in list(df.columns)]
            columns = df.columns
            df.dropna(axis=0, how='all', inplace=True)
            df.dropna(axis=1, how='all', inplace=True)
            # df['row_id'] = df.index + len(config['header_list']) + 2
            df['row_id'] = df.index + config['header_list'][-1] + 2
            for col_ in ['Sl.', 'Particulars']:
                try:
                    df[col_].fillna(method='ffill', inplace=True)
                except:
                    pass
            if len(config['append']) > 0:
                append_df = pd.read_excel(vc_model_raw, sheet_name=name, engine='pyxlsb', header=None)[
                    config['append'][0]:config['append'][1]]
                append_df.columns = columns
                append_df['row_id'] = append_df.index + 1
                df = df.append(append_df, ignore_index=True)
            df['plant_name'] = plant_name
            try:
                engine.execute("delete from "+"[vc_model" + name.split('.')
                            [-1] + "] where plant_name='" + plant_name + "'")
            except Exception as e:
                print(e)
            df.to_sql(name="vc_model" + name.split('.')
                      [-1], con=engine, index=False, if_exists='append')
    except Exception as e:
        logging.error("Error in the df_to_csv function", e)


def dateparse1(listx, format_, ctype):
    rest = ''
    try:
        if ctype == 'many':
            x = listx[1]
            rest = str(listx[0]) + "_"
        else:
            x = listx
        new_date = dt.date(1899, 12, 30) + dt.timedelta(days=float(x))
        rest + new_date.strftime(format=format_)
        return rest + new_date.strftime(format=format_)
    except:
        new_date = x
        return new_date


def vc_input_full_load(vc_input_raw, engine):
    try:
        config_all = {'2.Clk-Input': {'header_list': [0, 1], 'format_list': '%b-%y', 'ctype': 'many'},
                      '3.Cem-Input': {'header_list': [0, 1], 'format_list': '%b-%y', 'ctype': 'many'},
                      '6.LS Cost': {'header_list': [1, 2], 'format_list': '%b-%y', 'ctype': 'many'},
                      '8.Slag': {'header_list': [1, 2], 'format_list': '%b-%y', 'ctype': 'many'},
                      '9.Flyash': {'header_list': [1, 2], 'format_list': '%b-%y', 'ctype': 'many'},
                      '9.1.Pond Ash': {'header_list': [1, 2], 'format_list': '%b-%y', 'ctype': 'many'},
                      '10.Coal_Petcoke': {'header_list': [1, 2], 'format_list': '%b-%y', 'ctype': 'many'},
                      '10.1.Petcoke': {'header_list': [1, 2], 'format_list': '%b-%y', 'ctype': 'many'},
                      '11.1.GF_RGP': {'header_list': [2], 'format_list': '%b-%y', 'ctype': 'single'},
                      '11.2.GF_DDSPL': {'header_list': [1], 'format_list': '%b-%y', 'ctype': 'single'},
                      '11.3.GF_DPM': {'header_list': [1], 'format_list': '%b-%y', 'ctype': 'single'},
                      '11.4.GF_ALR': {'header_list': [1], 'format_list': '%b-%y', 'ctype': 'single'},
                      '11.5.GF_KDP': {'header_list': [1], 'format_list': '%b-%y', 'ctype': 'single'},
                      '11.6.GF_BGM': {'header_list': [1], 'format_list': '%b-%y', 'ctype': 'single'},
                      '11.7.GF_MGH': {'header_list': [1], 'format_list': '%b-%y', 'ctype': 'single'},
                      '11.8.GF_USO': {'header_list': [1], 'format_list': '%b-%y', 'ctype': 'single'}, }
        for name, config in config_all.items():
            print(name)
            df = pd.read_excel(vc_input_raw, sheet_name=name,
                               engine='pyxlsb', header=config['header_list'])
            df.columns = [dateparse1(
                x, config['format_list'], config['ctype']) for x in list(df.columns)]
            df.dropna(axis=0, how='all', inplace=True)
            df.dropna(axis=1, how='all', inplace=True)
            df['row_id'] = df.index + len(config['header_list']) + 2
            for col_ in ['Sl.', 'Particulars']:
                try:
                    df[col_].fillna(method='ffill', inplace=True)
                except:
                    pass
            df.drop(columns=[i for i in df.columns if i.startswith(
                "Unnamed:")], inplace=True)
            df.to_sql(name="vc_input" + name.split('.')
                      [-1], con=engine, index=False, if_exists='replace')

    except Exception as e:
        logging.error("Error in the df_to_csv1 function", e)


def main(plant_name):
    try:
        params = urllib.parse.quote_plus(
            "DRIVER={ODBC Driver 17 for SQL Server};SERVER=dalmia.database.windows.net;DATABASE=dalmiapoc;UID=dalmia;PWD=Dalpoc@123")
        engine = sqlalchemy.create_engine(
            "mssql+pyodbc:///?odbc_connect=%s" % params)
    except Exception as e:
        logging.error(str(e))
    try:
        with open('config.json') as f:
            data = json.load(f)
        variable_excel = data["variable_cost"]
        vc_input = data["vc_input"]
        vc_model_raw = pd.ExcelFile(variable_excel, engine='pyxlsb')
        vc_input_raw = pd.ExcelFile(vc_input, engine='pyxlsb')
    except Exception as e:
        logging.error("Error in fetching Variable Cost excel file", e)
    try:
        vc_model_full_load(vc_model_raw, engine, plant_name)
    except Exception as e:
        logging.error("Error in reading vc_model_full_load function", e)
    try:
        vc_input_full_load(vc_input_raw, engine)
    except Exception as e:
        logging.error("Error in fetching Variable Cost excel file", e)
    try:
        engine.dispose()
    except Exception as e:
        logging.error(str(e))


if __name__ == "__main__":
    main()
