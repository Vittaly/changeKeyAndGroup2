# -*- coding: utf-8 -*-
__author__ = 'Vitaly.Burkut'

#################### version history ###########################################
################################################################################
# 0.1 Created

################################################################################

###############################  libs  #########################################
#from datetime import datetime, timedelta
import os
from os.path import basename, isfile
import sys
import time
#from optparse import OptionParser
import logging

import pyodbc
#import dbf

from shutil import copyfile, move
#import csv
from queue import Queue
from threading import Thread


########################################################################################################################
############################### Constants ##############################################################################
########################################################################################################################
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
NEW_DIR  = os.path.join(BASE_DIR, "new")
RES_DIR = os.path.join(BASE_DIR, "results")
OLD_DIR  = os.path.join(BASE_DIR, "proccesed")
BAD_DIR  = os.path.join(BASE_DIR, "bad")
TEMP_DIR  = os.path.join(BASE_DIR, "temp")
EMPTY_DB_FULL_FN = os.path.join(BASE_DIR, "empty_db_for_copy.mdb")
CORRESPONDENCE_FILE = "Corrrespondance.accdb"
CORRESPONDENCE_FILE_FN =  os.path.join(BASE_DIR, CORRESPONDENCE_FILE)
CORRESPONDENCE_TABLE_NAME = 'CORR1'


appName = os.path.splitext(basename(__file__))[0]
logger = logging.getLogger(appName)


########################################################################################################################
#############################  Functions  ##############################################################################
########################################################################################################################

def open_access_conect(p_file_name):
    conn_str = r'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}}; DBQ={0}'.format(p_file_name)
    return pyodbc.connect(conn_str)

def create_empty_file_connect(p_full_file_name):
    copyfile(EMPTY_DB_FULL_FN, p_full_file_name)
    return  open_access_conect(p_full_file_name)


def create_table(p_connect):
    p_connect.execute ("create table table1 (id VARCHAR(20) PRIMARY KEY, Valeur1 VARCHAR(20), Valeur2 VARCHAR(20), Valeur3 VARCHAR(20), Valeur4 VARCHAR(20), Valeur5 VARCHAR(20), Valeur6 VARCHAR(20));")
    p_connect.commit()
    #PRIMARY KEY
def deleteDuplicateID(p_conn, p_tab_name):
  cur = p_conn.cursor()
  cur.execute('delete distinctrow c.*  from {0} c inner join  (select id from {0} group by id having count(*) > 1) d on c.id = d.id'.format(p_tab_name))
  p_conn.commit()

def addPK(p_conn, p_table = 'table1', p_fields = ['ID']):
    p_conn.execute('alter table {0} ADD PRIMARY KEY ({1});'.format(p_table, ','.join(p_fields)))


def table_struct_isCorrect(p_conn, p_table_name = 'table1'):
    try:
        addPK(p_conn, p_table_name)
    except Exception as pe:
        if pe.args[0] == "42S02":
           logger.error('Table {0} not exists in DB file'.format(p_table_name))
           return (False, pe.args[0])
        elif type(pe) == pyodbc.Error and  pe.args[0] == "HY000":
            logger.info("PK is already exists. Error:{0}".format(pe.args[1]))
            return (True, pe.args[0])
        elif type(pe) == pyodbc.IntegrityError and  pe.args[0] == "23000":
            logger.error("Was found duplicated ID in table {0}. Original error: {1}".format(p_table_name, pe.args[1]))
            return (False, pe.args[0])
        else:
            logger.error(" Error:{0}".format(pe.args[1]))
            return (False, pe.args[0])

    p_conn.commit();
    logger.warn('PK was not exists. Created')
    return (True, '')

def get_table_rec_count(p_con, p_table_name = 'table1'):
    cur = p_con.cursor()
    res = cur.execute('select count(*) from {}'.format(p_table_name))
    return res.fetchone()[0]
def merga_data_in_mdb(p_conn):
    cur = p_conn.cursor()
    res = cur.execute('select count(*) from table1;')
    val = res.fetchone()
    old_row_count = val[0]
    logger.info('Rows count in table1 before merge: {0}'.format(val[0]))
    res = cur.execute('select count(*) from tmp;')
    val = res.fetchone()
    logger.info('Rows count in temp table before merge: {0}'.format(val[0]))
    res = cur.execute('select id, count(*) from tmp group by id having count(*) > 1;')
    val = res.fetchall()
    if val == None:
        logger.info('Duplicate IDs in additional rows not found')
    else:
        logger.warn('Duplicate IDs in additional rows was found')
        for r in val:
            logger.warn('Duplicated id in new data: id:{0} count:{1}'.format(r[0], r[1]))
            logger.warn('Deleting...')
            cur.execute('delete from tmp where id = ? and pk not in (select min(pk) from tmp tt where tt.id = ?);', r[0], r[0])
        p_conn.commit()


    cur.execute('select id from tmp t where t.id in (select id from table1);')

    for r in cur:
        logger.warn('In new data was found id that already exists in table1: {0}'.format(r.id))
    cur.execute('insert into table1 select id, Valeur1, Valeur2, Valeur3, Valeur4, Valeur5, Valeur6  from tmp t where t.id not in (select id from table1);')
    p_conn.commit()
    res = cur.execute('select count(*) from table1;')
    val = res.fetchone()
    new_row_count = val[0]
    logger.info('Rows count in table1 after merge: {0}. {1} rows has added'.format(new_row_count, new_row_count - old_row_count))
    p_conn.execute('drop table tmp;')
    p_conn.commit()






def write_to_mdb(p_conn, p_tmp_tab_name, p_row_count):
    SMALL_PART_SIZE = 10

    cur = p_conn.cursor()

    HasDuplicate  = False

    ##cmd = 'insert into table1 select *  from [{0}] in "{1}"[Text;FMT=Delimited;HDR=YES] where id not in (select id from table1);'.format(p_tmp_tab_name , TEMP_DIR)
    cmd = 'insert into table1 select *  from [{0}] in "{1}"[Text;FMT=Delimited;HDR=YES];'.format(p_tmp_tab_name , TEMP_DIR)
    logger.debug('try insert {0} rows from the tmp file:{1}'.format(p_row_count, p_tmp_tab_name))

    try:
        cur.execute(cmd)
    except pyodbc.IntegrityError as pe:
        if pe.args[0] != '23000':
            logger.warn('Error then was insert into TABLE1: {0}'.format(pe))
            raise
        else:
            HasDuplicate = True
            logger.warn('Found PK duplicated in bulk insert. Try insert by small part ({0} rows)'.format(SMALL_PART_SIZE))
            if p_row_count <= SMALL_PART_SIZE:
                cmd = ' select *  from [{0}] in "{1}"[Text;FMT=Delimited;HDR=YES];'.format(p_tmp_tab_name , TEMP_DIR)
                rows =  cur.execute(cmd).fetchall()
                for rec in rows:
                    try:
                        cur.execute('insert into table1 values(?, ?, ?, ?, ?, ?, ?)', [x for x in rec] + [None for i in range(7 - len(rec))])
                    except  pyodbc.IntegrityError as pe2:
                        if pe2.args[0] == '23000':
                            logger.warn('Found PK duplicated for ID "{0}". Row ignored'.format(rec[0]))
                        else:
                            p_conn.rollback()
                            raise
            else: # if part is big then split it
                logger.warn('size of part biger then small part size({0}). it will doing split'.format(SMALL_PART_SIZE))
                fn = os.path.join(TEMP_DIR, p_tmp_tab_name)
                fn1 = os.path.splitext(p_tmp_tab_name)[0] + '_1.csv'
                fn2 = os.path.splitext(p_tmp_tab_name)[0] + '_2.csv'
                logger.warn('split {0}, to {1} and {2}'.format(p_tmp_tab_name, fn1, fn2))

                orig_file  = open(fn, 'r')
                reader = csv.reader(orig_file)
                row = next(reader)
                part_size = p_row_count / 2
                row_parts = [[],[]]
                for i, row in enumerate( reader):
                    if i <= part_size:
                        row_parts[0].append(row)
                    else:
                        row_parts[1].append(row)
                orig_file.close()
                fn1 = os.path.splitext(p_tmp_tab_name)[0] + '_1.csv'
                fn2 = os.path.splitext(p_tmp_tab_name)[0] + '_2.csv'
                put_recorss_to_csv_file(fn1,row_parts[0])
                put_recorss_to_csv_file(fn2,row_parts[1])

                logger.warn('was builed files {0} ({1} rows) and {2} ({3} rows)'.format(fn1, len(row_parts[0]), fn2, len(row_parts[1])))

                write_to_mdb(p_conn, fn1, len(row_parts[0]))
                write_to_mdb(p_conn, fn2, len(row_parts[1]))

                fn1f =  os.path.join(TEMP_DIR,  fn1)
                logger.debug('delete tmp file {0}'.format(fn1f))
                os.remove(fn1f)

                fn2f =  os.path.join(TEMP_DIR,  fn2)
                logger.debug('delete tmp file {0}'.format(fn2f))
                os.remove(fn2f)
    p_conn.commit()






def checkTableInFile(p_db_file_fn, p_table_name = 'Table1', p_delete_duplicate = False):
    if not os.path.isfile(p_db_file_fn):
        return False
    try:
        connect = open_access_conect(p_db_file_fn)
    except Exception as e:
        logger.error('error create conenct to access file {0}'.format(p_db_file_fn))
        return False
    check_result, err_code =  table_struct_isCorrect(connect, p_table_name)
    if not check_result and err_code == "23000" and p_delete_duplicate:
        logger.warn('duplicating records will be deleted from file {0}'.format(p_db_file_fn))
        deleteDuplicateID(connect, p_table_name)
        logger.info('duplicating records was deleted.')
        logger.info('Try create PK again')
        check_result, err_code =  table_struct_isCorrect(connect, p_table_name)
    if p_table_name == 'Table1' and check_result:
        check_result = checkOriginTable(connect)
    connect.close()
    return  check_result





def check_dirs():
    for dn in [NEW_DIR, RES_DIR, OLD_DIR, BAD_DIR, TEMP_DIR]:
        if not os.path.exists(dn):
            os.makedirs(dn)


def checkOriginTable(p_conn):

         cur = p_conn.cursor()

         dataFields = ['valeur{0}'.format(i) for i in list(range(1,7))]
         filter_sniplet =  ['{0} is not null and not IsNumeric({0})'.format(f) for f in dataFields]

         sql = 'select * from table1 t where t.id not in (\'Flop_Turn_River_Hand\') and ({0})'.format(' or '.join(filter_sniplet))

         cur.execute(sql)
         FindBadRec = False

         for row in cur.fetchall():
            if not FindBadRec: FindBadRec = True
            logger.error('find record with not number value: {}'.format(row))

         return not FindBadRec

def process_mdb_file(p_mdb_file):

    logger.info('start of processiong file {0}'.format(p_mdb_file))
    res_file_fn =  os.path.join(RES_DIR,p_mdb_file)


    copyfile(EMPTY_DB_FULL_FN, res_file_fn)
    logger.debug('created new empty file {0}'.format(res_file_fn))
    conn = open_access_conect(CORRESPONDENCE_FILE_FN)
    logger.debug('open connect to {0}'.format(CORRESPONDENCE_FILE_FN))
    cur = conn.cursor()

    sql = '''select id, a1 as valeur1, a2 as Valeur2, a3 as Valeur3, a4 as Valeur4, a5 as Valeur5, a6 as Valeur6
               into [MS Access; DATABASE={0};].table1
               from (select c.name as id, avg(valeur1) as a1, avg(valeur2) as a2, avg(valeur3) as a3, avg(valeur4) as a4, avg(valeur5) as a5, avg(valeur6) as a6
                       from [MS Access; DATABASE={1}].table1 t
                        inner join CORR1 c ON t.id = c.id
                        group by c.name
                        union all
                     select id, valeur1, valeur2, valeur3, valeur4, valeur5, valeur6
                       from [MS Access; DATABASE={1}].table1 t where not exists (select id from CORR1 cc where cc.id = t.id)
                        ) order by id'''.format(res_file_fn,  os.path.join(NEW_DIR, p_mdb_file))



    try:
        logger.debug('start proccessing data ...')
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        logger.error('Error on data processing: {0}'.format(e))
        conn.rollback()
        conn.close()

        logger.info('end of processiong file {0}'.format(p_mdb_file))
        return False


    res_conn =  open_access_conect(res_file_fn)
    res = res_conn.cursor().execute('select count(*) from table1').fetchval()
    logger.info('was added {0} reords to file {1}'.format(res, res_file_fn))
    conn.close()
    logger.info('Create PK for table1 in the file {0}'.format(res_file_fn))
    try:
        addPK(res_conn)
        res_conn.commit()
    except Exception as pe:
        if type(pe) == pyodbc.Error and  pe.args[0] == "HY000":
            logger.info('PK for table {} found. Check complite'.format(p_table))
        res_conn.close()
    res_conn.close()
    logger.info('end of processiong file {0}'.format(p_mdb_file))

    return True















########################################################################################################################
#####################################  Program  ########################################################################
########################################################################################################################

def main(argv):
    try:



        logFileName = os.path.join(BASE_DIR, appName + '.log')

        ##logging.basicConfig(filename=logFileName, level=logging.DEBUG, format='%(asctime)s %(message)s')



        hdlr = logging.FileHandler(logFileName)
        formatter = logging.Formatter('%(asctime)s %(levelname)s [%(threadName)s] %(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.DEBUG)

        logger.info('Starting...')
        logger.info('Check dirs exists')
        check_dirs()
        logger.info('Check Corresponndance db...')

        if not checkTableInFile(CORRESPONDENCE_FILE_FN, CORRESPONDENCE_TABLE_NAME, True):
            logger.error('Correct correspondation db file not found. Work break.')
            return 101





        logger.info('Check new files exists...')
        new_files = [f for f in os.listdir(NEW_DIR) if f.endswith(".mdb") or f.endswith(".accdb") if os.path.isfile(os.path.join(NEW_DIR, f))]

        if len(new_files) == 0:
            logger.info('New files not found')
            exit(0)
        logger.info('Found {0} files'.format(len(new_files)))


        #build maping
##        KEY_MAP = {}
##        con = open_access_conect(CORRESPONDENCE_FILE_FN)
##        cur = con.cursor()
##        cur.execute('select id, name from corr1')
##        row = cur.fetchone()
##        while row is not None:
##            # do something
##            KEY_MAP[row[0]] = row[1]
##            row = cur.fetchone()
##        con.close()



        for nf in new_files:
            nf_fn = os.path.join(NEW_DIR, nf)

            logger.info('Check the file: {0}'.format(nf))

            if checkTableInFile(nf_fn):
                process_mdb_file(nf)
                move(nf_fn, os.path.join(OLD_DIR, nf))
                logger.debug('the file {0} moved to dir {0}'.format(OLD_DIR))
            else:
                logger.error('file {0} if bad moving to dir {1}'.format(nf, BAD_DIR))
                move(nf_fn, os.path.join(BAD_DIR, nf))
                logger.debug('the file {0} moved to dir {0}'.format(BAD_DIR))



        logger.info('No more files. Stoping...')





    except Exception as e:
        logger.error('Error type %s : %s', str(type(e)), str(e))
        raise(e)

    logger.info('Ending...')
    logging.shutdown()


if __name__ == "__main__":
        sys.exit(main(sys.argv))