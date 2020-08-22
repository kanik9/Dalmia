import os
import json
import logging
import datetime
import sqlalchemy
import pandas as pd
import urllib.parse as up

from sqlalchemy import exc
from sqlalchemy import event
from mysite.settings import MEDIA_ROOT



def update(a):
    updated_columns = []
    for i in range(len(a)):
        if type(a[i]) == str:
            updated_columns.append(a[i])
        elif type(a[i]) == datetime.datetime:
            head_date = a[i].strftime("%b_%y")
            string = str(head_date)
            updated_columns.append(string)
        else :
            pass
    return updated_columns


def type_convert(data, columns):
    for i in range(len(columns)):
        if data[columns[i]].dtype == int:
            data[columns[i]] = data[columns[i]].astype(float)
        else:
            pass
    return data


def id_to_name(id , keys,engine):
    try:
        if keys == "ncr_input":
            query = """
                            SELECT  Region_Name
                            from  EBITDA_MST_REGION
                            WHERE  Region_Id = {}
    
                        """.format(id)
            conn = engine.connect()
            region_name = conn.execute(query).fetchone()
            return region_name[0]

        elif keys == "fc_model" or "vc_model":
            query = """
                        SELECT  Plant_Short_Name
                        from  EBITDA_MST_PLANT
                        WHERE  Plant_Id = {}
                    """.format(id)
            conn = engine.connect()
            plant_name = conn.execute(query).fetchone()
            return plant_name[0]
    except exc.SQLAlchemyError as e:
        print("Error : " + str(e))




def fc_file(id, engine, file_path):
    try:
        data = pd.read_excel(file_path, header=0)
    except Exception as e:
        if str(e) == "Excel 2007 xlsb file; not supported":
            data = pd.read_excel(file_path, engine="pyxlsb")
        else:
            error = """
                        PLease Provide Supports extension files  xls, xlsx, xlsm, xlsb, odf, ods and odt file extensions
                        """
            logging.error(error)
            raise e
    keys = "fc_model"
    data['Plant_Name'] = id_to_name(id, keys, engine)
    data = data.fillna(0)
    columns = list(data.columns)
    new_columns = update(columns)
    data = type_convert(data, columns)
    data.columns = new_columns
    data = data
    print(data)
"""
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True
    print("Inside the Sql")
    #data.to_sql('fc_model_summary_test_kanik', engine, index=False, if_exists="append", schema="dbo", chunksize=500)
"""


def vc_model_file(id, engine, file_path):
    try:
        data = pd.ExcelFile(file_path)
    except Exception as e:
        if str(e) == "Excel 2007 xlsb file; not supported":
            data = pd.read_excel(file_path, engine="pyxlsb")
        else:
            error = """
                        PLease Provide Supports extension files  xls, xlsx, xlsm, xlsb, odf, ods and odt file extensions
                        """
            logging.error(error)
            raise e

    keys = "vc_model"
    plant_name = id_to_name(id, keys, engine)
    sheet_name = ['2.Clk-Input', '3.Cem-Input', '6.LS Cost', '7.Power Mix', '8.Slag', '9.Flyash', '9.1.Pond Ash',
                  '10.Coal_Petcoke', '10.1.Coal_Petcoke Rate', '11.1.GF_RGP', '12.Stock']
    for name in sheet_name:
        if name == '2.Clk-Input':
            data_sheet = pd.read_excel(data, sheet_name=name, skiprows=8, index_col=False, header=1)

            data_sheet['Plant_Name'] = plant_name
            data_sheet = data_sheet.fillna(0)
            columns = list(data_sheet.columns)
            new_columns = update(columns)
            data_sheet = type_convert(data_sheet, columns)
            data_sheet.columns = new_columns
            data_sheet = data_sheet

            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True

            data_sheet.to_sql('vc_model_clk_input_test_kanik', engine, index=False, if_exists="replace", schema="dbo", chunksize=500)



        elif name == '3.Cem-Input':
            data_sheet = pd.read_excel(data, sheet_name=name, skiprows=10, index_col=False, header=1)
            data_sheet['Plant_Name'] = plant_name

            data_sheet = data_sheet.fillna(0)
            columns = list(data_sheet.columns)
            new_columns = update(columns)
            data_sheet = type_convert(data_sheet, columns)
            data_sheet.columns = new_columns
            data_sheet = data_sheet

            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True

            data_sheet.to_sql('vc_model_cem_input_test_kanik', engine, index=False, if_exists="replace", schema="dbo", chunksize=500)




        elif name == '6.LS Cost':
            data_sheet = pd.read_excel(data, sheet_name='6.LS Cost', skiprows=2, index_col=False, header=0)
            data_sheet['Plant_Name'] = plant_name
            data_sheet = data_sheet.fillna(0)
            columns = list(data_sheet.columns)
            new_columns = update(columns)
            data_sheet = type_convert(data_sheet, columns)
            data_sheet.columns = new_columns
            data_sheet = data_sheet

            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True

            data_sheet.to_sql('vc_model_ls_cost_test_kanik', engine, index=False, if_exists="replace", schema="dbo", chunksize=500)

        elif name == '7.Power Mix':
            data_sheet = pd.read_excel(data, sheet_name='7.Power Mix', skiprows=3, index_col=False, header=0)
            data_sheet['Plant_Name'] = plant_name
            data_sheet = data_sheet.fillna(0)
            columns = list(data_sheet.columns)
            new_columns = update(columns)
            data_sheet = type_convert(data_sheet, columns)
            data_sheet.columns = new_columns
            data_sheet = data_sheet


            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True


            data_sheet.to_sql('vc_model_power_mix_test_kanik', engine, index=False, if_exists="replace", schema="dbo", chunksize=500)

        elif name == '8.Slag':
            data_sheet = pd.read_excel(data, sheet_name='8.Slag', skiprows=2, index_col=False, header=0)
            data_sheet['Plant_Name'] = plant_name
            data_sheet = data_sheet.fillna(0)
            columns = list(data_sheet.columns)
            new_columns = update(columns)
            data_sheet = type_convert(data_sheet, columns)
            data_sheet.columns = new_columns
            data_sheet = data_sheet

            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True

            data_sheet.to_sql('vc_model_slag_test_kanik', engine, index=False, if_exists="replace", schema="dbo", chunksize=500)

        elif name == '9.Flyash':
            data_sheet = pd.read_excel(data, sheet_name='9.Flyash', skiprows=2, index_col=False, header=0)
            data_sheet['Plant_Name'] = plant_name
            data_sheet = data_sheet.fillna(0)
            columns = list(data_sheet.columns)
            new_columns = update(columns)
            data_sheet = type_convert(data_sheet, columns)
            data_sheet.columns = new_columns
            data_sheet = data_sheet

            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True

            data_sheet.to_sql('vc_model_flyash_test_kanik', engine, index=False, if_exists="replace", schema="dbo", chunksize=500)

        elif name == '9.1.Pond Ash':
            data_sheet = pd.read_excel(data, sheet_name='9.1.Pond Ash', skiprows=2, index_col=False, header=0)
            data_sheet['Plant_Name'] = plant_name
            data_sheet = data_sheet.fillna(0)
            columns = list(data_sheet.columns)
            new_columns = update(columns)
            data_sheet = type_convert(data_sheet, columns)
            data_sheet.columns = new_columns
            data_sheet = data_sheet

            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True

            data_sheet.to_sql('vc_model_pond_ash_test_kanik', engine, index=False, if_exists="replace", schema="dbo", chunksize=500)

        elif name == '10.Coal_Petcoke':
            data_sheet = pd.read_excel(data, sheet_name='10.Coal_Petcoke', skiprows=2, index_col=False, header=0)
            data_sheet['Plant_Name'] = plant_name
            data_sheet = data_sheet.fillna(0)
            columns = list(data_sheet.columns)
            new_columns = update(columns)
            data_sheet = type_convert(data_sheet, columns)
            data_sheet.columns = new_columns
            data_sheet = data_sheet

            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True

            data_sheet.to_sql('vc_model_coal_petcoke_test_kanik', engine, index=False, if_exists="replace", schema="dbo", chunksize=500)

        elif name == '10.1.Coal_Petcoke Rate':
            data_sheet = pd.read_excel(data, sheet_name='10.1.Coal_Petcoke Rate', skiprows=2, index_col=False, header=0)
            data_sheet['Plant_Name'] = plant_name
            data_sheet = data_sheet.fillna(0)
            columns = list(data_sheet.columns)
            new_columns = update(columns)
            data_sheet = type_convert(data_sheet, columns)
            data_sheet.columns = new_columns
            data_sheet = data_sheet

            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True

            data_sheet.to_sql('vc_model_coal_petcoke_Rate_test_kanik', engine, index=False, if_exists="replace", schema="dbo", chunksize=500)

        elif name == '11.1.GF_RGP':
            data_sheet = pd.read_excel(data, sheet_name='11.1.GF_RGP', skiprows=2, index_col=False, header=0)
            data_sheet['Plant_Name'] = plant_name
            data_sheet = data_sheet.fillna(0)
            columns = list(data_sheet.columns)
            new_columns = update(columns)
            data_sheet = type_convert(data_sheet, columns)
            data_sheet.columns = new_columns
            data_sheet = data_sheet

            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True

            data_sheet.to_sql('vc_model_gf_rgp_test_kanik', engine, index=False, if_exists="replace", schema="dbo", chunksize=500)

        elif name == '12.Stock':
            data_sheet = pd.read_excel(data, sheet_name='12.Stock', skiprows=2, index_col=False, header=0)
            data_sheet['Plant_Name'] = plant_name
            data_sheet = data_sheet.fillna(0)
            columns = list(data_sheet.columns)
            new_columns = update(columns)
            data_sheet = type_convert(data_sheet, columns)
            data_sheet.columns = new_columns
            data_sheet = data_sheet

            @event.listens_for(engine, "before_cursor_execute")
            def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
                    if executemany:
                        cursor.fast_executemany = True

            data_sheet.to_sql('vc_model_stock_test_kanik', engine, index=False, if_exists="replace", schema="dbo", chunksize=500)


def ncr_file(id, engine,file_path):
    try :
        data = pd.read_excel(file_path)
    except Exception as e:
        if str(e) == "Excel 2007 xlsb file; not supported":
            data = pd.read_excel(file_path, engine="pyxlsb")
        else:
            error = """
                        PLease Provide Supports extension files  xls, xlsx, xlsm, xlsb, odf, ods and odt file extensions
                        """
            logging.error(error)
            raise e
    keys = "ncr_input"
    data['Region_Name'] = id_to_name(id, keys, engine)

    data = data.fillna(0)
    columns = list(data.columns)
    data = type_convert(data, columns)
    data = data
    print(data)
    """
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
            if executemany:
                cursor.fast_executemany = True

    print("sql")
    data.to_sql('test_ncr_input_kanik', engine, index=False, if_exists="append", schema="dbo", chunksize=500)
    """
    print("complete")



def main(id, keys):
    try:
        print("in main function")
        with open('config.json') as f:
            data = json.load(f)
        params = up.quote("DRIVER={" + str(data['DRIVER']) + "};SERVER=" + str(data['SERVER']) + ";DATABASE=" + str(data['database_name']) + ";UID=" + str(data['uid']) + ";pwd=" + str(data['pwd']))
        engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))

        key_function = {'fc_model': fc_file, 'vc_model': vc_model_file, "ncr_input": ncr_file}
        function = key_function.get("%s" % keys)
        base_path = data.get("%s" % keys)
        file_path = os.path.join(MEDIA_ROOT, '{}'.format(base_path))
        function(id, engine, file_path)
        engine.dispose()



    except Exception as e:
        print(str(e))

