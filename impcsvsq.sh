#!/bin/sh

#Script to load CSV to oracle using external tables
#Assumptions:
# 1. Should be run as ORACLE owner on target server
# 2. Folder containing CSV should be owned by Oracle
# 3. SQL*Plus should be available on the server
# 4. Sugar schema user should have DBA privileges


CSV_SAVE_SPACE="${CSV_SAVE_SPACE:-'yes'}"
ORA_USER="${ORA_USER:-'sugar'}"
ORA_PWD="${ORA_PWD:-'sugar'}"
#ORA_TNS=
ORA_FORMAT=${ORA_FORMAT:-'YYYY-MM-DD HH24:MI:SS'}
CSV_DIR="${CSV_DIR:-'/u01/app/oracle/admin/orcl/csv'}"


#connect string depending on tns
if [ -z ${ORA_TNS}  ]; then
 connect=${ORA_USER}/${ORA_PWD}
else
 connect=${ORA_USER}/${ORA_PWD}@${ORA_TNS}
fi


dpause(){
# read -p "Press any key to proceed"
:
}

#fail quickly
fail() {
 echo -e '\E[31m'"\033[1m"$1"\033[0m"
 exit -1
}


#echo begin section
echo_bn() {
   echo -n "$1"
}

#echo and section
echo_en() {
     echo -e '\E[32m'"\033[1m"$1"\033[0m"
}

#start timing
timing_b() {
 SECONDS=0
}

#end timing after timeb()
timing_e() {
 duration=$SECONDS
 echo "Time elapsed: $(($duration / 60))m $(($duration % 60))s"
}

#number of parallel processes depending on CPU count
pll() {
  p=`nproc`
 if [ ${p} -lt 2 ]; then
  p=2
 fi
 echo ${p}
}

query(){
# $1 query to execute
sqlplus -silent /nolog << EOF
 SET PAGESIZE 0 FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF LINESIZE 30000
 CONNECT ${connect}
 $1;
 EXIT;
EOF
}

#Routine to create Oracle directory object
#Should point at oracle owner readable OS directory
create_ora_directory() {
 sqlplus -silent /nolog << EOF
  CONNECT ${connect}
  DROP DIRECTORY sugar_imp;
  CREATE DIRECTORY sugar_imp as '${CSV_DIR}';
  GRANT READ, WRITE ON DIRECTORY sugar_imp TO PUBLIC;
  EXIT;
EOF
 ret=`query "select 0 from dba_directories where directory_name='SUGAR_IMP'"`
 [ ${ret} -eq 0 ] || fail "Failed to create directory. Check permissions of oracle user $ORA_USER"
}

split_csv(){
#$1 - csv file name
#Splits csv to chunks required for fast parallel loading
#Number of chunks is equail to number of cpu or at least 2 

 n_cpu=`pll`
 n_spl=n_cpu
 declare -i n_lines=`tail -n +2 $1 | wc -l`
 let n_spl=n_lines/n_cpu+1
 tail -n +2 $1 | split -l ${n_spl} - $1_chunk_
 echo `ls -mQ $1_chunk_* | sed -r 's/"/'\'''\''/g'`
}

clear_csv(){
# $1 - csv file name
 rm -f $1_chunk_*
}


type_col() {
#$1 table
#$2 column
#Query for column data type
#
sqlplus -silent /nolog << EOF
SET PAGESIZE 0 FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF
CONNECT ${connect}
select column_name||' '||
decode(data_type,'NUMBER', 'NUMBER',
                  'VARCHAR2', 'VARCHAR2('||DATA_LENGTH||')',
                  'CHAR','CHAR('||DATA_LENGTH||')',
                  'CLOB','VARCHAR2(4000)',
                  'BLOB','VARCHAR2(4000)',
                  'DATE','VARCHAR2(32)')
as coldef 
from user_tab_columns
where table_name=UPPER('$1') and column_name=UPPER($2);
EXIT;
EOF
}

table_col() {
#$1 table
#$2 headers
#Creating list of columns with data types from csv header
for h in $(echo $2| sed "s/,/ /g"); do
 echo `type_col $1 ${h}`,
done
}

tab_create(){
#$1 - csv
#$2 - table
#$3 - headers

#Creates external Oracle table that can be feed with csv
#Table name is: %tablename%_ext (e.g. calls_ext)

 csv=`basename ${1}`
 parallel=`pll`
 table=$2
 files=`split_csv ${csv}`
 headers=$3
 columns=`table_col ${table} ${headers}`

dummy=`query "drop table ${table}_ext"`
sqlplus -silent /nolog  << EOF
SET PAGESIZE 0 FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF
CONNECT ${connect}
DECLARE
 v_sql VARCHAR2(4000);
BEGIN
 v_sql := 'CREATE TABLE ${table}_ext ( ';	
 v_sql := v_sql || '${columns}';
 v_sql := RTRIM(v_sql, ',') || ') ';
 v_sql := v_sql || ' ORGANIZATION EXTERNAL (TYPE ORACLE_LOADER DEFAULT DIRECTORY sugar_imp  ACCESS PARAMETERS (';
 v_sql := v_sql || ' RECORDS DELIMITED BY NEWLINE ';
 v_sql := v_sql || ' BADFILE sugar_imp:''${table}_ext.bad'' ';
 v_sql := v_sql || ' LOGFILE sugar_imp:''${table}_ext.log'' ';
 v_sql := v_sql || ' FIELDS TERMINATED BY '','' OPTIONALLY ENCLOSED BY X''27'' ';
 v_sql := v_sql || ' MISSING FIELD VALUES ARE NULL) ';
 v_sql := v_sql || ' LOCATION (${files}) ) ';
 v_sql := v_sql || ' PARALLEL ${parallel} ';
 v_sql := v_sql || ' REJECT LIMIT UNLIMITED ';
 EXECUTE IMMEDIATE v_sql;
END;
/
EXIT;
EOF

}

#TO_DO Rewrite to save all unique indexes and constraints
prepare_table(){
# $1 table
# stores pk and unique index definitions in a table
dummy=`query "create table sugar_pku_t(t varchar2(30), c varchar2(30), def varchar2(4000))"`
sqlplus -silent  /nolog  << EOF
SET PAGESIZE 0 FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF
CONNECT ${connect}
begin

  /* no logging on table */
  execute immediate 'alter table $1 nologging parallel';

  /* saving unique indexes and constraints definition */
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SEGMENT_ATTRIBUTES',false);
  DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'PRETTY',FALSE);
  for idx in (select i.table_name,
                     i.index_name,
                     c.constraint_name,
                     c.constraint_type,
                     DBMS_LOB.SUBSTR(DBMS_METADATA.GET_DDL('INDEX', i.index_name),4000, 1) as definition
               from user_indexes i, user_constraints c
                  where i.table_name=c.table_name
                  and i.index_name=c.index_name
                  and i.UNIQUENESS = 'UNIQUE'
                  and i.table_name=UPPER('${1}')) loop

    merge into sugar_pku_t spu using
      (select idx.table_name t, idx.constraint_name c, idx.definition def from dual) tab
       on
      (spu.t = tab.t and spu.c = tab.c)
      when matched then
       update set def = tab.def
      when not matched then
       insert(spu.t, spu.c, spu.def) values (tab.t, tab.c, tab.def);
      commit;
      execute immediate 'alter table ${1} disable constraint '|| idx.constraint_name || ' drop index';
  end loop;

  /* disabling indexes */
  for i in (select index_name from user_indexes where table_name=upper('$1') and  index_type<>'LOB') loop
   execute immediate 'alter index '||i.index_name||' unusable';
  end loop;

  /* disabling constraints if any */
  for c in (select constraint_name from user_constraints where table_name=upper('$1') and index_name is null) loop
   execute immediate 'alter table $1 disable constraint '||c.constraint_name;
  end loop;

  /* disabling LOB cache and logging (not sure it should be enabled) */
  for l in (select column_name from user_lobs where table_name=upper('$1')) loop
   execute immediate 'alter table $1 modify lob('||l.column_name||')(NOCACHE NOLOGGING)';
  end loop;
end;
/
EXIT;
EOF
}



load_table(){
# $1 table
# $2 columns
# Load table in parallel mode by appending records

table=$1
columns=$2

sqlplus -silent  /nolog  << EOF
SET PAGESIZE 0 FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF
CONNECT ${connect}
ALTER SESSION SET NLS_DATE_FORMAT='${ORA_FORMAT}';
ALTER SESSION ENABLE PARALLEL DML;
ALTER SESSION SET SKIP_UNUSABLE_INDEXES=TRUE;
INSERT /*+ PARALLEL(${table}) */ INTO ${table}(${columns})
SELECT /*+ PARALLEL(${table}_EXT) */ ${columns} FROM ${table}_EXT;
COMMIT;
ALTER SESSION SET SKIP_UNUSABLE_INDEXES=FALSE;
EXIT;
EOF
}

post_load_table(){
# $1 table
# Performs post action on table, Post action should be written in SQL in a file named as a table
# with extension .pst
if [ -e ./POST/${1}.pst ]; then
 echo -n "Post action found for '$1'"
sqlplus -silent  /nolog  << EOF
CONNECT ${connect}
@./POST/${1}.pst
EXIT;
EOF
echo -n "Post action complete. Check output above "
 else
echo -n "No post action for '$1' "
fi
}

rebuild_idx(){
# $1 table
# Rebuilding indexes in nologging and parallel mode
# PK should be rebuild separately using _rebuild_pk procedure 
parallel=`pll`
sqlplus -silent  /nolog  << EOF 
SET PAGESIZE 0 FEEDBACK OFF VERIFY OFF HEADING OFF ECHO OFF
CONNECT ${connect}
ALTER SESSION ENABLE PARALLEL DML;
begin

 for p in (select c ,def from sugar_pku_t where t=upper('$1')) loop
  execute immediate 'alter table $1 enable constraint '||p.c||' using index ('||p.def||' NOLOGGING PARALLEL ${parallel})';
 end loop;

 for i in (select index_name from user_indexes where table_name=upper('$1') and  index_type<>'LOB'and status='UNUSABLE') loop
   execute immediate 'alter index '||i.index_name||' rebuild nologging parallel ${parallel}';
 end loop;

 for i in (select index_name from user_indexes where table_name=upper('$1') and  index_type<>'LOB') loop
   execute immediate 'alter index '||i.index_name||' noparallel';
 end loop;

 for c in (select constraint_name from user_constraints where table_name=upper('$1') and status='DISABLED') loop
   execute immediate 'alter table $1 enable constraint '||c.constraint_name;
 end loop;

 for l in (select column_name from user_lobs where table_name=upper('$1')) loop
  execute immediate 'alter table $1 modify lob('||l.column_name||')(NOCACHE LOGGING)';
 end loop;

 execute immediate 'alter table $1 noparallel logging';
end;
/
EXIT;
EOF
}

process_csv_batch(){


  timing_b
  dpause
  echo "Generating external tables"
  for csv in ${CSV_DIR}/*.csv
  do

   headers_q=`head -n 1 ${csv}`
   base_csv=$(basename "$csv")
   table="${base_csv%.*}"

   echo_bn "Table ${table}_ext ... "
   tab_create ${csv} ${table} ${headers_q}
   ret=`query "select 1 from user_external_tables where table_name=UPPER('${table}_ext')"`
        if [ ${ret} !=  "1" ]; then
            echo_en "failed"
            continue
        fi
   echo_en "done"

  done #generating ex tables

  dpause
  echo ""
  echo "Preparing tables for data load"
  for csv in ${CSV_DIR}/*.csv
  do
   base_csv=$(basename "$csv")
   table="${base_csv%.*}"

   echo_bn "Preparing table ${table} ... "
   prepare_table ${table}
   echo_en "done"

  done #disabling constraints

  dpause
  echo ""
  echo "Loading table data"
  for csv in ${CSV_DIR}/*.csv
  do
   base_csv=$(basename "$csv")
   table="${base_csv%.*}"
   headers_nq=`head -n 1 ${csv} | sed -r "s/'//g"`

   echo_bn "Loading table ${table} ... "
   load_table ${table} ${headers_nq}
   echo_en "done"

  done #loading data


  dpause
  echo ""
  echo "Performing post actions"
  for csv in ${CSV_DIR}/*.csv
  do
   base_csv=$(basename "$csv")
   table="${base_csv%.*}"
   echo_bn "Post load actions for ${table} ... "
   post_load_table ${table}
   echo_en "done"
  done #post processing


  dpause
  echo ""
  echo "Rebuilding indexes and constraints"
  for csv in ${CSV_DIR}/*.csv
  do
   base_csv=$(basename "$csv")
   table="${base_csv%.*}"
   echo_bn "Rebuilding indexes for ${table} ... "
   rebuild_idx ${table}
   echo_en "done"
  done


  dpause
  echo ""
  echo "Cleaning up"
  for csv in ${CSV_DIR}/*.csv
  do
   base_csv=$(basename "$csv")
   table="${base_csv%.*}"
   echo_bn "Removing csv and external table ${table}_ext ... "
   clear_csv ${base_csv}
   dummy=`query "drop table ${table}_ext"`
   echo_en "done"
  done
  timing_e

}


process_csv(){
 for csv in ${CSV_DIR}/*.csv
 do

    echo "============================================"
    echo "Processing file: $csv"
        timing_b
    echo "============================================"

    base_csv=$(basename "$csv")
    table="${base_csv%.*}"

    #headers with single quote
    headers_q=`head -n 1 ${csv}`
    headers_nq=`head -n 1 ${csv} | sed -r "s/'//g"`

    #creating oracle external table that will point
    #at our split csv files
    echo_bn "1. Creating external table '${table}_ext'..."
        tab_create ${csv} ${table} ${headers_q}
    echo_en "done"
    dpause

    echo_bn "2. Checking if table '${table}_ext' has been created..."
        ret=`query "select 1 from user_external_tables where table_name=UPPER('${table}_ext')"`
        if [ ${ret} !=  "1" ]; then
            echo_en "failed, skipping to the next item"
            continue
        fi
    echo_en "done"
    dpause

    echo_bn "3. Disabling constraints for '$table'... "
        prepare_table ${table}
    echo_en "done"
    dpause

    echo_bn "4. Loading records into '$table'... "
        load_table ${table} ${headers_nq}
    echo_en "done"
    dpause

    echo_bn "5. Performing post actions for '$table'..."
        post_load_table ${table}
    echo_en "done"
    dpause
 
    echo_bn "6. Rebuilding indexes (it might take a while)..."
        rebuild_idx ${table}
    echo_en "done"
    dpause

    echo_bn "7. Cleaning up..."
        clear_csv ${base_csv}
        dummy=`query "drop table ${table}_ext"`
    echo_en "done"
    dpause

    #####
    timing_e
    #####

    echo "Done with: $csv"
    echo "============================================"
done
}

start=`date +%s`
create_ora_directory

if [ ${CSV_SAVE_SPACE} = "yes" ]; then
  process_csv
else
  process_csv_batch
fi

end=`date +%s`
runtime=$((end-start))
echo "==================================="
echo "Execution time is: $runtime seconds"
echo "==================================="
exit 0
