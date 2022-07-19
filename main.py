import re
import gspread
import os
import json
import psycopg2
from datetime import datetime, timezone
import requests
import xml.etree.ElementTree as ET


def read_value(input_name):
    '''
    Читаем из файла последнюю дату и время обновления файла для предотвращения действий если файл не был обновлен
    :param input_name: название файла в который пишем
    :return: Дата посл
    '''
    with open(input_name, 'r') as input_file:
        data = input_file.read()
        if data:
            data = float(data)

    return data

def write_value(input_name, value_upd):
    '''
    Пишем в файл дату и время последнего обновления файла для предотвращения действий если файл не был обновлен
    :param input_name: имя файла
    :return: None
    '''
    with open(input_name, 'w') as input_file:
        input_file.truncate(0)
        input_file.write(str(value_upd))

work = True
try:
    while work:
        r = read_value('last_upd')


        def get_cur_usd():
            '''
            Получаем данные актуального курса USD.
            Получаю регулярным выражением, так как в данном случае решил что это будет быстрее чем работать с xml данными
            :return: курсе USD с плавающей точкой
            '''
            URL = 'http://www.cbr.ru/scripts/XML_daily.asp'
            resp = re.search(r'Доллар США.*?(?P<USD>\d*,\d*)</Value>', requests.get(URL).text).groupdict()
            return float(resp['USD'].replace(',','.'))

        def dictfetchall(cursor):
            '''
            Преобразуем данные с БД в список, для удобности проверки изменений
            :param cursor:
            :return: список объектов в Бд
            '''
            columns = [col[0] for col in cursor.description]
            result_dict = {}
            for row in cursor.fetchall():
                itm = dict(zip(columns,row))
                result_dict[itm['order_num']] = itm
            return result_dict

        USD_VAL = get_cur_usd()
        conn = psycopg2.connect(dbname='test_database', user='postgres',
                                password='pgpwd4habr', host='localhost')
        cursor = conn.cursor()
        h_d = os.getcwd()
        sa = gspread.service_account(filename=h_d+'/sa.json')
        sh = sa.open_by_key('1Ip22F6-Zfzm-qhz8XF-FpVlIrfQSsqkuf11_c-3EDYI')
        last_update_ts = datetime.timestamp(datetime.strptime(sh.lastUpdateTime, '%Y-%m-%dT%H:%M:%S.%fZ'))


        if last_update_ts != r:
            ps_select_query = """ SELECT * FROM test_schema.table_name"""
            cursor.execute(ps_select_query)
            all_data = dictfetchall(cursor) # Получаем данные с БД
            wks = sh.worksheet('Лист1')
            lst = wks.get_all_records() # Получаем данные с Листа 1 GOOGLE api
            update_list = [] # словарь для обновления строк
            insert_list = [] # словарь для добавления строк
            using_order = [] # словарь для сравнения используемых данных и данных бд
            delete_list = [] # словарь для удаления
            for itm_sh in lst:
                if all_data.get(itm_sh['заказ №']):

                    using_order.append(int(itm_sh['заказ №']))
                    data_object = all_data.get(itm_sh['заказ №'])
                    date_time_itm_sh = datetime.strptime(itm_sh['срок поставки'], '%d.%m.%Y')
                    date_time_itm_sh = date_time_itm_sh.date()
                    if data_object['num'] == itm_sh['№'] and data_object['order_num'] == itm_sh['заказ №'] and data_object['delivery_period'] == date_time_itm_sh and data_object['price_dollar'] == itm_sh['стоимость,$']:
                        pass
                    else:
                        postgres_update_q =  """UPDATE test_schema.table_name SET price_dollar = %s, delivery_period= %s   WHERE order_num = %s;"""
                        date_time_obj = datetime.strptime(itm_sh['срок поставки'], '%d.%m.%Y')
                        data_str = f"{date_time_obj.year}/{date_time_obj.month}/{date_time_obj.day}"
                        update_list.append((itm_sh['стоимость,$'], data_str,itm_sh['заказ №']))
                else:
                    date = itm_sh['срок поставки']
                    price_rub = int(itm_sh['стоимость,$']) * USD_VAL
                    date_time_obj = datetime.strptime(date, '%d.%m.%Y')
                    data_str = f"{date_time_obj.year}/{date_time_obj.month}/{date_time_obj.day}"
                    insert_list.append((itm_sh['№'], itm_sh['заказ №'], itm_sh['стоимость,$'], data_str, price_rub))
                    postgres_insert_query = """ INSERT INTO test_schema.table_name(num, order_num, price_dollar,delivery_period, price_rub) VALUES (%s,%s,%s,%s,%s)"""
            for num, itm in enumerate(all_data):
                if itm not in using_order:
                    postger_delete_query = """DELETE FROM test_schema.table_name WHERE order_num = %s"""
                    delete_list.append((itm,))
            if len(update_list):
                cursor.executemany(postgres_update_q,update_list)
            if len(insert_list):
                cursor.executemany(postgres_insert_query, insert_list)
            if len(delete_list):
                cursor.executemany(postger_delete_query, delete_list)
            conn.commit()
            write_value('last_upd', last_update_ts)
        else:
            print('Last upd Ok')
except KeyboardInterrupt:
    cursor.close()
    conn.close()
    work = False