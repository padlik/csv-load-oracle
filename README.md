# Csv-load-oracle
Bash script to import CSV into Oracle database using [External Tables][1]. Script is designed for using with SugarCRM but can be adopted for virtually any environment.

##Usage
Copy script to a folder containing CSV files and run it as oracle owner (typically oracle) 

##Concept
Scripit is based on the concept recommended by Oracle for large data loading operations:
- Split CSVs by number or CPU available
- Create external tables of SQL*Loader for parallel load
- Disable all possible contraints. 
- Save Primary Key definition in a temporary table and remove primary key
- Load data in parallel using all the CPU available
- Post-process data when required
- Enable indexes, constraints and primary key

##CSV file format
Due to some limitations, CSV format accepted by the script is not classical one. There are some limitations:
- Values are separated by **single** quotes (char 39)
- CSV file name should be in lower case and should be equail to the apropriate table name in the database 
- First row of the CSV file should contain column names of the target table
- CSV should consists of single line rows

##Processing notes
As soon as External tables should be available to Oracle Instance directly, CSV files should be located on the same server as the database or should be shared via NFS, Samba or whatever. Directory with CSV files should be **at least** readable by Oracle user owner (typically it's oracle:oinstall). Oracle user running import should have appropriate privileges to create directories, external tables, disable constraints and etc. 

##Disk space requirements
As soon as script splits CSV files to chunks per number of CPU, directory should provide free space enough for storing all CSVs plus size of largest CSV plus 10%

##Parameters
- `CSV_SAVE_SPACE` - Experimental, processes tables in a batch,  when set to **no**  
- `ORA_USER` - Oracle database user
- `ORA_PWD` - Oracle database user password
- `ORA_TNS` - Oracle TNS alias.
- `ORA_FORMAT` - Oracle date format using for import dates: default is: `'YYYY-MM-DD HH24:MI:SS'`
- `CSV_DIR` - Directory of script and files

##Post-processing
Sometimes it is required to post-process data loaded. Script allows to execute sql statements **after** data load and **before** enabling constraints. Script should be named as `{table_name}.pst`  and placed under ./POST subfolder.  For example It can be useful for updating columns populated by sequences.
[1]:https://docs.oracle.com/cd/B19306_01/server.102/b14215/et_concepts.htm
