'''
This script will import all databases and tables without any content from a mysql server to another mysql server
The storage engine on the target mysql server is BLACKHOLE, you can use this server as a binary log server
'''

import MySQLdb

'''
Congfiguration for source mysql server
'''
source_host = '10.18.138.190'
source_port = 3306
source_user = 'root'
source_pass = 'P@ssw0rd'

'''
Configuration for target mysql server
'''
target_host = '10.18.138.172'
target_port = 3306
target_user = 'root'
target_pass = 'P@ssw0rd'

def execute(sql, host, port=3306, 
            user='root', passwd='', db=''):
    conn = MySQLdb.connect(host=host, port=port, user=user, 
                           passwd=passwd, db=db)
    cursor = conn.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

sourcedbs = [row['Database'] for row in execute('show databases', 
                                                source_host,
                                                source_port, 
                                                source_user, 
                                                source_pass)]

targetdbs = set([row['Database'] for row in execute('show databases', 
                                                    target_host,
                                                    target_port, 
                                                    target_user, 
                                                    target_pass)]) 
for database in sourcedbs:
    
    if (database == 'mysql' or 
        database == 'test' or 
        database == 'information_schema'):
        continue

    if database in targetdbs:
        print 'drop old database %s' % database
        execute('drop database %s' % database,
                target_host,
                target_port,
                target_user,
                target_pass)

    print 'creating database %s' % database
    
    execute('create database %s' % database,
            target_host,
            target_port,
            target_user,
            target_pass)

    rows = execute('show tables', 
                   source_host, 
                   source_port, 
                   source_user, 
                   source_pass, database)
    tables = [row['Tables_in_%s' % database ] for row in rows]
    for table in tables:
        table_sql = execute('show create table %s' % table,
                            source_host,
                            source_port,
                            source_user,
                            source_pass, database)[0]['Create Table']
        table_sql = table_sql.replace('MyISAM', 
                                      'BLACKHOLE').replace('InnoDB', 
                                                           'BLACKHOLE')
        print 'creating table %s.%s' % (database, table)

        execute(table_sql,
                target_host,
                target_port,
                target_user,
                target_pass, database)

