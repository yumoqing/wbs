# test
"""The application's model objects"""
from odbcutils.odbc import opendb,execSQL

createTableSQL = """
create table batchstatus
(
    id int,
    batchdate varchar(8),
    name varchar(1000),
    sysgroup varchar(400),
    sysid int,
    starttime datetime,
    endtime datetime,
    status int,
    cancel int,
    primary key(id)
)"""


def dbinit(dsn,ctSQL):
	conn = opendb(dsn)
	if ctSQL is None:
		ctSQL = createTableSQL
	execSQL(conn,ctSQL)
	conn.close()