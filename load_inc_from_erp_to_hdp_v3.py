#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 19 11:44:47 2018

@author: olga
"""

import pyrfc
import pandas as pd
import configparser
from datetime import datetime
import hdfs
import logging


def print_params():
    logging.info(u'Loading increment...')
    logging.info(u'subscriber type: ' + queue_params['I_SUBSCRIBER_TYPE'])
    logging.info(u'subscriber name: ' + queue_params['I_SUBSCRIBER_NAME'])
    logging.info(u'subscriber process: ' + queue_params['I_SUBSCRIBER_PROCESS'])
    logging.info(u'queue name: ' + queue_params['I_QUEUENAME'])
    logging.info(u'extraction mode: ' + queue_params['I_EXTRACTION_MODE'])
    
    
def get_details():
    '''
    Получение деталей о структуре возвращаемых данных:
    наименования колонок, типы, длина выводимого поля в строке и т.д. 
    I_SUBSCRIBER_TYPE - тип системы-подписчика. Мы пока используем SAP BO
    I_QUEUENAME - имя очереди (экстрактора)
    '''
    return conn.call(u"RODPS_REPL_SOURCE_GET_DETAIL",
                     I_SUBSCRIBER_TYPE = queue_params['I_SUBSCRIBER_TYPE'],
                     I_QUEUENAME = queue_params['I_QUEUENAME'])


def open_queue():
    '''
    Открывает очередь ODQ в ERP. Параметры необходимо передавать в юникоде: u'BLABLLA'.
    Наименования и значения параметров регистрозависимы.
    
    I_SUBSCRIBER_TYPE - тип системы-подписчика. Мы пока используем SAP BO
    I_SUBSCRIBER_NAME - имя системы-подписчика
    I_SUBSCRIBER_PROCESS - имя процесса
    I_QUEUENAME - имя очереди (экстрактора)
    I_EXTRACTION_MODE - режим экстракции: D - дельта
    
    Возвращаемое значение:
    Список структур, самая важная из которых -
    E_POINTER - дата в формате Decimal(23,9)
    '''
    return conn.call(u"RODPS_REPL_SOURCE_OPEN",
                     I_SUBSCRIBER_TYPE = queue_params['I_SUBSCRIBER_TYPE'],
                     I_SUBSCRIBER_NAME = queue_params['I_SUBSCRIBER_NAME'],
                     I_SUBSCRIBER_PROCESS = queue_params['I_SUBSCRIBER_PROCESS'],
                     I_QUEUENAME = queue_params['I_QUEUENAME'], 
                     I_EXTRACTION_MODE = queue_params['I_EXTRACTION_MODE'])


def fetch_rows(open_res):
    '''
    Вычитывает строки из очереди ODQ в ERP.
    Данные возвращаются строками определенной длины. Если строка таблицы не поместилась в эту длину,
    то продолжение переносится на следующую строку и значение флага CONTINUATION = Х
    
    I_POINTER - дата в формате Decimal(23,9), на которую будет найден соответствующий пакет с данными.
    Результат метода RODPS_REPL_SOURCE_OPEN
    I_MAXPACKAGESIZE - максимальное количество строк в пакете. По умолчанию 52428800
    I_ENCODING - кодировка. По умолчанию UTF-8
    
    Возвращаемое значение:
    Список структур, самая важная из которых -
    ET_DATA - данные 
    '''
    return conn.call('RODPS_REPL_SOURCE_FETCH', 
                   I_POINTER = open_res['E_POINTER'],
                   I_MAXPACKAGESIZE=1,
                   I_ENCODING='UTF-8')


def concatenate_rows(fetch_res):
    '''
    Данные из метода RODPS_REPL_SOURCE_FETCH возвращаются строками определенной длины. Если строка таблицы не поместилась в эту длину,
    то продолжение переносится на следующую строку и значение флага CONTINUATION = Х
    Функция склеивает куски строк в одну и складывает в виде списка.
    
    Возвращаемое значение:
    res_rows - список строк возвращенной таблицы без разделителей
    '''
    res_rows = []
    new_row = b''
    num = 0
    for line in fetch_res['ET_DATA']:
        if line['CONTINUATION'] == 'X':
            new_row = new_row + line['DATA']
        else:
            new_row = new_row + line['DATA']
            res_rows.append(new_row.decode('utf-8').replace('\x00', ''))
            new_row = b''
            num += 1
    return res_rows


def parse_row(row):
    '''
    Выделение из строки значений колонок на основании порядка колонок и длины, полученных из
    метода RODPS_REPL_SOURCE_GET_DETAIL
    
    Возвращаемое занчение:
    res_cols - словарь. Ключ - название колонки, Значение - значение колонки
    '''
    global details
    res_cols = {}
    cur_idx = 0
    for field in details['ET_FIELDS']:
        end_idx = cur_idx + int(field['OUTPUTLENG'])
        res_cols[field['NAME']] = row[cur_idx:end_idx].strip()
        cur_idx = end_idx
    return res_cols


def save_to_file(ready_df, j):
    local_file_name = folder + '/' + queue_params['I_QUEUENAME'] + datetime.now().strftime('%Y%m%d') + '.csv'

    logging.info('Writing file to local file system: {}'.format(local_file_name))
    ready_df.to_csv(local_file_name, index = False)
    logging.info('Writing to local system is finished. File {}'.format(local_file_name))

    new_file_name = queue_params['I_QUEUENAME'] + datetime.now().strftime('%Y%m%d') + '_' + str(j) + '.csv'
    logging.info('Writing file to HDFS: {}'.format(new_file_name))

    client.upload('inc/{}/{}'.format(queue_params['I_QUEUENAME'], new_file_name), local_file_name, n_threads=10)

    logging.info('Writing to HDFS is finished. File ' + new_file_name)        
    logging.info('Size: ' + str(ready_df.shape[0]) + ', ' + str(ready_df.shape[1]))


if __name__ == '__main__':
    # размер блока на Hadoop
    #block_size = 128*1024*1024.0

    folder = '/home/local/X5/olga.guzeva/inc/2LIS_02_ITM'

    # init
    queue_params = {'I_SUBSCRIBER_TYPE': u'BOBJ_DS',
                    'I_SUBSCRIBER_NAME': u'PYTHON',
                    'I_SUBSCRIBER_PROCESS': u'PYTHON',
                    'I_QUEUENAME': u'2LIS_02_ITM',
                    'I_EXTRACTION_MODE': u'D'
                    }


    # Logging
    logging.basicConfig(format = u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s',
        filename = u'/home/local/X5/olga.guzeva/logs/' + queue_params['I_QUEUENAME'] + '_' + datetime.now().strftime('%Y%m%d') + '_log.log', level = logging.INFO)

    # Подключение к HDFS
    client = hdfs.Config().get_client('dev')

    logging.info('************Start program************')

    print_params()
    
    # загружаем конфиг с параметрами для подключения
    config = configparser.ConfigParser()
    config.read('/home/local/X5/olga.guzeva/cfg/sapnwrfc.cfg')
    cfg_sap = config['SAP_ER1']
    
    # Устанавливаем коннект и печатаем статус
    conn = pyrfc.Connection(**cfg_sap)
    result = conn.call('STFC_CONNECTION', REQUTEXT='Hello SAP!')
    logging.info(result)
    logging.info('Connaction alive = ' + str(conn.alive))
    
    # Получаем структуру возвращаемого датасета
    # logging.info(datetime.now())
    logging.info('Getting details...')
    details = get_details()
    
    # Открываем очередь
    # logging.info(datetime.now())
    logging.info('Openning queue...')
    open_result = open_queue()
    
    # DataFrame будет хранить строки для записи в один файл размером с блок на Hadoop
    batch_df = pd.DataFrame()
    # Используется для подсчета текущего размера DataFrame
    current_size = 0
    # Количество строк, которое влезет в блок
    rows_cnt = 200000
    # Счетчик для добавления в имя файла
    i = 1
    j = 1
    # Вычитываем строки из очереди в цикле
    # Выходим из цикла, когда данные закончились: E_NO_MORE_DATA = X
    # logging.info(datetime.now())
    logging.info('Fetching queue...')
    while True:
        fetch_result = fetch_rows(open_result)
        if fetch_result['E_NO_MORE_DATA'] == 'X':
            save_to_file(batch_df, j)
            logging.info('E_NO_MORE_DATA = True')
            break

        # Сохраняем промежуточный результат в файл
        tmp_file_name = file_name = 'inc/tmp_' + queue_params['I_QUEUENAME']

        with client.write(tmp_file_name, overwrite=True) as writer:
            writer.write(bytes(str(fetch_result['ET_DATA']), 'utf-8'))
            logging.info('Writing of temp file is finished. File ' + tmp_file_name)

        # Склеиваем части строк. См. описание к функции 
        raw_rows = concatenate_rows(fetch_result)
    
        clear_rows = list(map(parse_row, raw_rows))
        res_df = pd.DataFrame(clear_rows)

        logging.info('batch_df.shape[0] = ' + str(batch_df.shape[0]))
        logging.info('res_df.shape[0] = ' + str(res_df.shape[0]))
        logging.info('rows_cnt = ' + str(rows_cnt))
        if batch_df.shape[0] + res_df.shape[0] < rows_cnt:
            batch_df = batch_df.append(res_df, sort=False)
        else:
            if batch_df.shape[0] == 0:
                batch_df = batch_df.append(res_df, sort=False)
                    
            save_to_file(batch_df, j)
            
            batch_df = res_df
            j += 1           
             
        i += 1

    logging.info('************End program************')