'''
Fast copy innodb database 

By Frank Fang (@FrankFang)

This script is used to copy mysql innodb database to another one,
becuase mysqldump is slow, this script copy idb files directly instead

'''

import sys
import os
import commands


import MySQLdb
import paramiko


'''
configuration for mysql
'''

source_host = '10.18.138.190'
source_port = 3306
source_user = 'root'
source_pass = 'P@ssw0rd'
source_database = 'amoeba'
source_datadir = '/var/lib/mysql'
source_ssh_user = 'root'
source_ssh_pass = 'P@ssw0rd'

target_host = '10.18.138.170'
target_port = 3306
target_user = 'root'
target_pass = 'P@ssw0rd'
target_database = 'amoeba'
target_datadir = '/var/lib/mysql'


def write_spaceid(ibd_file_path, spaceid):
    ibd_file = open(ibd_file_path, 'r+b')

    spaceid_high = (spaceid >> 8) & 0xff
    spaceid_low = spaceid & 0xff

    ibd_file.seek(36)
    ibd_file.write("".join([chr(spaceid_high), chr(spaceid_low)]))

    ibd_file.seek(40)
    ibd_file.write("".join([chr(spaceid_high), chr(spaceid_low)]))

    ibd_file.close()


def parse_spaceid(ibd_file_path):
    ibd_file = open(ibd_file_path, 'r')
    
    ibd_file.seek(36)
    pos37_high = ord(ibd_file.read(1))
    pos37_low = ord(ibd_file.read(1))

    spaceid37 = (pos37_high << 8) + pos37_low

    ibd_file.seek(40)
    pos40_high = ord(ibd_file.read(1))
    pos40_low = ord(ibd_file.read(1))

    spaceid40 = (pos40_high << 8) + pos40_low

    if spaceid37 != spaceid40:
        ibd_file.close()
        raise Exception('position 37 is %x, position 40 is %x, not equal' % 
                        (spaceid37, spaceid40))

    ibd_file.close()
    return spaceid37

def scp(ssh_host, ssh_user, ssh_pass, 
        remote_file, local_file):
    ssh_client = paramiko.SSHClient()
    ssh_client.load_host_keys(os.path.expanduser(os.path.join("~", 
                                                              ".ssh", 
                                                              "known_hosts")))
    if ssh_pass is None or len(ssh_pass) == 0:
        ssh_client.connect(ssh_host, username=ssh_user)
    else:
        ssh_client.connect(ssh_host, username=ssh_user, password=ssh_pass)

    sftp_client = ssh_client.open_sftp()
    sftp_client.get(remote_file, local_file)
    sftp_client.close()
    ssh_client.close()

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


'''
create target database
'''

print 'creating target database %s' % target_database

execute("create database if not exists %s" % target_database,
        target_host,
        target_port,
        target_user,
        target_pass)

'''
get source database tables
'''

rows = execute('show tables',
               source_host,
               source_port,
               source_user,
               source_pass,
               source_database)

tables = [row['Tables_in_%s' % source_database ] for row in rows]

for table in tables:

    '''
    create same table in target database
    '''
    table_create_sql = execute("show create table %s" % table,
                               source_host,
                               source_port,
                               source_user,
                               source_pass,
                               source_database)[0]['Create Table']
    #print 'creating table %s in %s' % (table, target_database)
    execute(table_create_sql,
            target_host,
            target_port,
            target_user,
            target_pass,
            target_database)

    '''
    parse new table's space id
    '''
    spaceid = parse_spaceid('%s/%s/%s.ibd'% (target_datadir,
                                            target_database,
                                            table))
    #print 'new table\'s space id is %d' % spaceid

    '''
    discard new table's tablespace
    '''
    execute('alter table %s discard tablespace' % table,
            target_host,
            target_port,
            target_user,
            target_pass,
            target_database)

    '''
    scp table ibd file from remote server
    '''
    remote_ibd_file = "%s/%s/%s.ibd" % (source_datadir, source_database, table)
    local_ibd_file = "%s/%s/%s.ibd" % (target_datadir, target_database, table)
    #print 'scp %s to %s' % (remote_ibd_file, local_ibd_file)
    scp(source_host, source_ssh_user, source_ssh_pass, 
        remote_ibd_file, local_ibd_file)

    commands.getoutput('chown mysql:mysql %s' % local_ibd_file)
    commands.getoutput('chmod 660 %s' % local_ibd_file)

    '''
    write table space id
    '''
    #print 'writing space id %d to %s' % (spaceid, local_ibd_file)
    write_spaceid(local_ibd_file, spaceid)

    '''
    import table space
    '''
    #print 'importing %s table space' % table
    execute('alter table %s import tablespace' % table,
            target_host,
            target_port,
            target_user,
            target_pass,
            target_database)

    print 'finish copy %s spaceid is %d' % (table, spaceid)
