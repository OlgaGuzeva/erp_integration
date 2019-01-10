#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec 19 11:44:47 2018

@author: olga
"""

import pandas as pd
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



def save_to_file(ready_df, j):
    local_file_name = folder + '/' + queue_params['I_QUEUENAME'] + datetime.now().strftime('%Y%m%d') + '.csv'

    logging.info('Writing file to local file system: {}'.format(local_file_name))
    ready_df.to_csv(local_file_name, index=False)
    logging.info('Writing to local system is finished. File {}'.format(local_file_name))

    new_file_name = queue_params['I_QUEUENAME'] + datetime.now().strftime('%Y%m%d') + '_' + str(j) + '.csv'
    logging.info('Writing file to HDFS: {}'.format(new_file_name))

    client.upload('inc/{}/{}'.format(queue_params['I_QUEUENAME'], new_file_name), local_file_name, n_threads=10)

    logging.info('Writing to HDFS is finished. File ' + new_file_name)
    logging.info('Size: ' + str(ready_df.shape[0]) + ', ' + str(ready_df.shape[1]))


if __name__ == '__main__':
    folder = '/Users/olga/workspace/tmp/'

    # init
    queue_params = {'I_SUBSCRIBER_TYPE': u'BOBJ_DS',
                    'I_SUBSCRIBER_NAME': u'PYTHON',
                    'I_SUBSCRIBER_PROCESS': u'PYTHON',
                    'I_QUEUENAME': u'2LIS_02_ITM',
                    'I_EXTRACTION_MODE': u'D'
                    }

    # Logging
    logging.basicConfig(format=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s',
                        filename=u'/Users/olga/workspace/tmp/' + queue_params[
                            'I_QUEUENAME'] + '_' + datetime.now().strftime('%Y%m%d') + '_log.log', level=logging.INFO)

    # Подключение к HDFS
    client = hdfs.Config().get_client('dev')

    logging.info('************Start program************')

    print_params()

    i = 1

    logging.info('Fetching queue...')
    for cur in range(3):

        res_df = pd.DataFrame([cur, cur])

        logging.info('res_df.shape[0] = ' + str(res_df.shape[0]))
        logging.info('rows_cnt = ' + str(rows_cnt))

        save_to_file(res_df, j)

        j += 1

    logging.info('************End program************')