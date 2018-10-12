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
import csv
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
CORRESPONDENCE_FILE = "Correspondance.accdb"
CATEGORIES_FILE = 'Output categories.txt'
CATEGORIES_FILE_FN = os.path.join(BASE_DIR, CATEGORIES_FILE)
CORRESPONDENCE_FILE_FN =  os.path.join(BASE_DIR, CORRESPONDENCE_FILE)
CORRESPONDENCE_TMP_FILE_FN = os.path.join(TEMP_DIR, 'tmp_' + CORRESPONDENCE_FILE + '.mdb')
CATEGORIES_LIST = []
CORRESPONDENCE_TABLE_NAME = 'corr1'

HEADER_IDS = ['Flop_hand', 'Flop_Turn_hand', 'Flop_Turn_River_hand']
HEADER_IDS_SQL =', '.join(['\'{0}\''.format(id) for id in HEADER_IDS])

VALUE_FIELD_PREF = 'Valeur'


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
    p_connect.execute ("create table table1 (id VARCHAR(200), {0}1 VARCHAR(500), {0}2 VARCHAR(500), {0}3 VARCHAR(500), {0}4 VARCHAR(500), {0}5 VARCHAR(500), {0}6 VARCHAR(500));".format(VALUE_FIELD_PREF))
    p_connect.commit()
    #PRIMARY KEY
def deleteDuplicateID(p_conn, p_tab_name):
  cur = p_conn.cursor()
  copyfile(EMPTY_DB_FULL_FN, CORRESPONDENCE_TMP_FILE_FN)

  cur.execute('select c.id, min(left(name,255)) as n into [MS Access;DATABASE={0}].corr_tmp  from {1} c group by id'.format(CORRESPONDENCE_TMP_FILE_FN,  p_tab_name))
  cur.execute('drop table {0}'.format(p_tab_name))
  cur.execute('select id, n as name into {0} from [MS Access;DATABASE={1}].corr_tmp'.format(p_tab_name, CORRESPONDENCE_TMP_FILE_FN))
  p_conn.commit()

def addPK(p_conn, p_table = 'table1', p_fields = ['ID']):
    p_conn.execute('alter table {0} ADD PRIMARY KEY ({1});'.format(p_table, ','.join(p_fields)))




def CheckPkInTable(p_conn, p_table_name = 'table1', p_pk_column = ['id']):
    try:
        addPK(p_conn, p_table_name, p_pk_column)
    except Exception as pe:
        if pe.args[0] == "42S02":
           logger.error('Table {0} not exists in DB file'.format(p_table_name))
           return 1
        elif type(pe) == pyodbc.Error and  pe.args[0] == "HY000":
            logger.info("PK is already exists. Error:{0}".format(pe.args[1]))
            return 0
        elif type(pe) == pyodbc.IntegrityError and  pe.args[0] == "23000":
            logger.error("Was found duplicated ID in table {0}. Original error: {1}".format(p_table_name, pe.args[1]))
            return 2
        else:
            logger.error(" Error:{0}".format(pe.args[1]))
            return (False, pe.args[0])

    p_conn.commit();
    logger.warn('PK was not exists. Created')
    return 0


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






def checkCorrespTable(p_db_file_fn):
    if not os.path.isfile(p_db_file_fn):
        return False
    try:
        connect = open_access_conect(p_db_file_fn)
    except Exception as e:
        logger.error('error create conenct to access file {0}'.format(p_db_file_fn))
        return False

    chk =  CheckPkInTable(connect, CORRESPONDENCE_TABLE_NAME)

    res = False

    if chk == 2:   #  was found duplicated record and need to delete it
        logger.warn('duplicating records will be deleted from file {0}'.format(p_db_file_fn))
        deleteDuplicateID(connect, CORRESPONDENCE_TABLE_NAME)
        logger.info('duplicating records was deleted.')
        logger.info('Try create PK again')
        if CheckPkInTable(connect, CORRESPONDENCE_TABLE_NAME) != 0:
            logger.info('Correspondent file is bad')
        else:
          res = True

    elif chk == 0:
        res = True

    connect.close()
    if os.path.exists(CORRESPONDENCE_TMP_FILE_FN):
        os.remove(CORRESPONDENCE_TMP_FILE_FN)

    return res


def checkDataInNewFile(p_db_file_fn):
    table_name = 'Table1'
    if not os.path.isfile(p_db_file_fn):
        return False
    try:
        connect = open_access_conect(p_db_file_fn)
    except Exception as e:
        logger.error('error create conenct to access file {0}'.format(p_db_file_fn))
        return False

    if CheckPkInTable(connect, table_name) != 0:
        connect.close()
        return False

    # check record in new table except for record with id not in corr1 table
    connect.close()

    res =  checkOriginTable(p_db_file_fn)
    connect.close()
    return  res





def check_dirs():
    for dn in [NEW_DIR, RES_DIR, OLD_DIR, BAD_DIR, TEMP_DIR]:
        if not os.path.exists(dn):
            os.makedirs(dn)


def checkOriginTable(p_new_file_fn):

         connect = open_access_conect(CORRESPONDENCE_FILE_FN)

         cur = connect.cursor()

         cnt = cur.execute('select count(*) from table1 where id in ({0})'.format(HEADER_IDS_SQL)).fetchval()
         if cnt > 1:
             logger.error('found more then one record with id: {}'.format(HEADER_IDS))
             return False


         dataFields = ['valeur{0}'.format(i) for i in list(range(1,7))]
         filter_sniplet =  ['{0} is not null and (not IsNumeric({0}) or Instr({0}, \'-\') > 0)'.format(f) for f in dataFields]

         sql = 'select * from [MS Access; DATABASE={2}].table1 t where t.id not in ({1}) and ({0}) and t.id in (select d.id from {3} d)'.format(' or '.join(filter_sniplet), HEADER_IDS_SQL, p_new_file_fn, CORRESPONDENCE_TABLE_NAME )

         cur.execute(sql)
         FindBadRec = False

         for row in cur.fetchall():
            if not FindBadRec: FindBadRec = True
            logger.error('find record with not number or negative value: {}'.format(row))

         connect.close()
         return not FindBadRec


def process_mdb_file(p_mdb_file):

    global CATEGORIES_LIST

    logger.info('start of processiong file {0}'.format(p_mdb_file))
    res_file_fn =  os.path.join(RES_DIR,p_mdb_file)
    orig_file_fn =  os.path.join(NEW_DIR, p_mdb_file)
    tmp_csv_file =  p_mdb_file + '.csv'


    #Preccess 1: find id in proccesing file and define sql for grouping value fields
    orig_file_conn =  open_access_conect(orig_file_fn)

    row = orig_file_conn.execute('select * from table1 where id in ({0})'.format(HEADER_IDS_SQL)).fetchone()

    group_col_head = ['RAISE', 'CALL', 'BET']

    group_col_head_pr = [i[0:3] for i in group_col_head]
    new_head_row = []
    select_field = {}

    for i, f in enumerate(row):
        if f == None:
            continue
        if i == 0:
            new_head_row.append(f)
            continue
        if f.startswith(group_col_head_pr[0]):
            if not group_col_head[0] in new_head_row : new_head_row.append(group_col_head[0])
            if not group_col_head[0] in select_field:
                select_field[group_col_head[0]] = ['Val({0})'.format(VALUE_FIELD_PREF + str(i))]
            else:
                select_field[group_col_head[0]].append('Val({0})'.format(VALUE_FIELD_PREF + str(i)))
        elif f.startswith(group_col_head_pr[1]):
            if not group_col_head[1] in new_head_row : new_head_row.append(group_col_head[1])
            if not group_col_head[1] in select_field:
                select_field[group_col_head[1]] = ['Val({0})'.format(VALUE_FIELD_PREF + str(i))]
            else:
                select_field[group_col_head[1]].append('Val({0})'.format(VALUE_FIELD_PREF + str(i)))
        elif f.startswith(group_col_head_pr[2]):
            if not group_col_head[2] in new_head_row : new_head_row.append(group_col_head[2])
            if not group_col_head[2] in select_field:
                select_field[group_col_head[2]] = ['Val({0})'.format(VALUE_FIELD_PREF + str(i))]
            else:
                select_field[group_col_head[2]].append('Val({0})'.format(VALUE_FIELD_PREF + str(i)))

        elif f != None:
            new_head_row.append(f)
            if not f in new_head_row : new_head_row.append(group_col_head[2])
            if not f in select_field:
                select_field[f] = [VALUE_FIELD_PREF + str(i)]
            else:
                select_field[f].append(VALUE_FIELD_PREF + str(i))

    id_record_sql = ','.join(["'{}'".format(f) for f in new_head_row])
    if len(new_head_row) < 7:
         id_record_sql += ', ' + ', '.join(['Null' for i in range(7 - len(new_head_row))])

    #for i in range(len(select_field) ,6):
     #       select_field[str(i)] = ['Null']

    sql_fields = ', '.join([' + '.join(select_field[k]) + ' as v' + str(i+1) for i, k in enumerate(select_field.keys())])

    if len(select_field) < 6:
       sql_fields += ', ' + ', '.join(['Null as v{0}'.format(i) for i in range(len(select_field) + 1 ,7)])
    sql_p1 = 'select id, {0} from [MS Access; DATABASE={1}].table1 where id not in ({2})'.format(sql_fields, orig_file_fn, HEADER_IDS_SQL)

    orig_file_conn.close()



    #Preccess 1,2,3

    conn = open_access_conect(CORRESPONDENCE_FILE_FN)
    logger.debug('open connect to {0}'.format(CORRESPONDENCE_FILE_FN))
    cur = conn.cursor()





    with open(os.path.join(TEMP_DIR, 'schema.ini'),'w') as f:
        f.write('[{0}]\n'.format(tmp_csv_file))
        f.write('DecimalSymbol=.')


    sql = '''select id, IIF(isNull(a1), null, cInt(a1)) as valeur1, IIF(isNull(a2), null, cInt(a2)) as Valeur2, IIF(isNull(a3), null, cInt(a3)) as Valeur3, IIF(isNull(a4), null, cInt(a4)) as Valeur4, IIF(isNull(a5), null, cInt(a5)) as Valeur5, IIF(isNull(a6), null, cInt(a6)) as Valeur6
               into [Text;FMT=Delimited;HDR=YES; DATABASE={0};].[{1}]
               from (select c.name as id, avg(v1) as a1, avg(v2) as a2, avg(v3) as a3, avg(v4) as a4, avg(v5) as a5, avg(v6) as a6
                       from ({2}) t
                        inner join {3} c ON t.id = c.id
                        group by c.name
                        union all
                        select id, v1, v2, v3, v4, v5, v6
                       from ({2}) t where not exists (select id from {3} cc where cc.id = t.id)

                     )'''.format(TEMP_DIR, tmp_csv_file,  sql_p1, CORRESPONDENCE_TABLE_NAME)


    logger.debug(sql)

    try:
        logger.debug('start proccessing data ...')
        cur.execute(sql)
#!!!!!!        cur.execute("insert into [Text;FMT=Delimited;HDR=YES; DATABASE={0};].[{1}] values ({2})".format(TEMP_DIR, tmp_csv_file, id_record_sql))
        conn.commit()
    except Exception as e:
        logger.error('Error on data processing to csv: {0}'.format(e))
        conn.rollback()
        conn.close()

        logger.info('end of processiong file {0}'.format(p_mdb_file))
        return False


    logger.info('start process {0} file (after 3th process)'.format(tmp_csv_file))

    result_data = {}
    with open(os.path.join(TEMP_DIR, tmp_csv_file)) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        next(csv_reader)
        for rec in csv_reader:
            if '_' in rec[0]:
                key, postKey = rec[0].rsplit('_',1)
                try:
                    index = CATEGORIES_LIST.index(postKey)
                except Exception as e:
                    logger.warn('Second part of ID={0} not  found in category list. Record with id {1} set as is'.format(postKey, rec[0]))
                    index  = 0
                    key = rec[0]
            else:
               index  = 0
               key = rec[0]

            if not key in result_data:
                if key != rec[0]:
                    result_data[key] = [['' for i in range(len(CATEGORIES_LIST))] for k in range(len(new_head_row)-1)]
                else:
                    result_data[key] = [[''] for i in range(len(new_head_row)-1)] # if key not found in category list set record as is (only one element in array)
            buf_rec = result_data[key]
            for i in range(len(new_head_row)-1):
                if not rec[i+1]:
                    break
                buf_rec[i][index] = rec[i+1]

    out_csv_file = 'out_' + tmp_csv_file
    with open(os.path.join(TEMP_DIR, out_csv_file), 'w', newline='') as csvfile:
        csvWriter = csv.writer(csvfile, quoting=csv.QUOTE_NONNUMERIC)
        csvWriter.writerow(['ID', 'Valeur1' , 'Valeur2' , 'Valeur3', 'Valeur4', 'Valeur5', 'Valeur6'])
        nonAp =  [None]*(7-len(new_head_row))
        csvWriter.writerow(new_head_row + nonAp)
        for k, v in result_data.items():
            row = [k] + ['_'.join(i) for i in v]  + nonAp
            csvWriter.writerow(row)





    copyfile(EMPTY_DB_FULL_FN, res_file_fn)
    logger.debug('created new empty file {0}'.format(res_file_fn))

    res_conn =  open_access_conect(res_file_fn)
    logger.debug('open connect to {0}'.format(res_file_fn))


    #create_table(res_conn)
    try:
        logger.debug('move date from {0} to {1}'.format(out_csv_file, res_file_fn))
        res_conn.cursor().execute('select * into table1 from [Text;FMT=Delimited;HDR=YES; DATABASE={0};].[{1}]'.format(TEMP_DIR, out_csv_file))
        res_conn.commit()
    except Exception as e:
        logger.error('error while move:'.format(e))



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

    os.remove(os.path.join(TEMP_DIR, out_csv_file))
    os.remove(os.path.join(TEMP_DIR, tmp_csv_file))

    return True





def readCategories(p_file_fn):
    if not os.path.exists(p_file_fn):
        logger.error('File with categories list not exists. checked file {0}'.format(p_file_fn))
        return
    with open(p_file_fn) as f:
        return f.read().splitlines()









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

        if not checkCorrespTable(CORRESPONDENCE_FILE_FN):
            logger.error('Correct correspondation db file not found. Work break.')
            return 101
        global CATEGORIES_LIST
        CATEGORIES_LIST =  readCategories(CATEGORIES_FILE_FN)
        if not CATEGORIES_LIST:
            logger.error('Invalid categories list. Work break')
            return 102





        logger.info('Check new files exists...')
        new_files = [f for f in os.listdir(NEW_DIR) if f.endswith(".mdb") or f.endswith(".accdb") if os.path.isfile(os.path.join(NEW_DIR, f))]

        if len(new_files) == 0:
            logger.info('New files not found')
            exit(0)
        logger.info('Found {0} files'.format(len(new_files)))


        for nf in new_files:
            nf_fn = os.path.join(NEW_DIR, nf)

            logger.info('Check the file: {0}'.format(nf))

            if checkDataInNewFile(nf_fn):
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