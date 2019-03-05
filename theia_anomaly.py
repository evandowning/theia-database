import sys
import os
import logging
import time
import re
import uuid

import psycopg2

import sensitive

log = logging.getLogger(__name__)
log_format = '[%(asctime)s] [%(levelname)s]: %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format=log_format)

class TheiaAnomaly(object):

    def __init__(self, conf):
        # Initialize with config settings
        self.dbname = conf['dbname']
        self.username = conf['username']
        self.password = eval(conf['password'])
        self.server = conf['server']
        self.port = int(conf['port'])
        self.flush_time = int(conf['flush_time'])
        self.cache_max = int(conf['cache_max'])

        # Create cache for filenames (keyed on UUID)
        self.cache = dict()

        # Open database
        try:
            self.conn = psycopg2.connect(host=self.server, port=self.port, database=self.dbname, user=self.username, password=self.password)
        except (Exception, psycopg2.DatabaseError) as error:
            log.error(error)
            self.conn = None

        # Create cursor
        if self.conn is not None:
            self.cur = self.conn.cursor()

        # Get last time (for flushing periodically)
        self.last_time = time.time()

        # Create ignore list
        self.ignore_list =  [ "/dev/null", \
                              "/var/log/upstart/relay-read-file\.log", \
                              "/data/ahg\.dump\..*", \
                            ]

    # To sanitize string for inserting into PostgreSQL
    def sanitize(self, string):
        string = string.replace("\\", "\\\\")
        string = string.replace("'", "\\'")
        string = string.replace("\"", "\\\"")
        string = string.replace("\n", "\\n")

        return string

    # Objects to ignore
    def ignore(self, obj):
        rv = False

        for r in self.ignore_list:
            if re.match(r,obj) is not None:
                rv = True
                break

        return rv

    # Parse CDM data
    def parse(self, data):
        # Get ubiquitous CDM info
        cdm_type = data['type']
        cdm_uuid = data['datum']['uuid']
        cdm_host = data['hostId']
        uuid_str = str(uuid.UUID(bytes=cdm_host)) + '_' + str(uuid.UUID(bytes=cdm_uuid))

        # If no connection, no need to parse anything
        if self.conn is None:
            return

        # If it's an event
        if cdm_type == 'RECORD_EVENT':
            # Get parameters
            entry_type = data['datum']['type']

            predicate_uuid = data['datum']['predicateObject']
            p_uuid_str = str(uuid.UUID(bytes=cdm_host)) + '_' + str(uuid.UUID(bytes=predicate_uuid))

            # If this is a potentially sensitive event
            if entry_type == 'EVENT_READ' or \
               entry_type == 'EVENT_WRITE' or \
               entry_type == 'EVENT_OPEN' or \
               entry_type == 'EVENT_MMAP':

                filename = ''
                # Get filename from cache
                if p_uuid_str in self.cache:
                    filename,hits = self.cache[p_uuid_str]

                    # Increase popularity of cache hit
                    self.cache[p_uuid_str] = (filename,hits+1)

                # Get filename from database
                else:
                    tmp_cur = self.conn.cursor()

                    tmp_cur.execute("SELECT filename from file WHERE " \
                                    "uuid = '{0}';".format(p_uuid_str))

                    records = tmp_cur.fetchall()

                    # If no filename exists, then skip this event
                    if len(records) == 0:
                        return

                    # Else, get filename
                    for row in records:
                        filename = row[0]

                    tmp_cur.close()

                # If we should ignore this file, then skip this event
                if self.ignore(filename):
                    return

                # Check if this event is sensitive
                if self.sensitive(entry_type,filename):

                    # This event is sensitive, so insert it into the database
                    self.cur.execute("INSERT INTO event (uuid,type,file_uuid) " \
                                     "SELECT '{0}','{1}','{2}' " \
                                     "WHERE NOT EXISTS ( " \
                                     "SELECT uuid FROM event WHERE " \
                                     "uuid='{0}');".format(uuid_str,entry_type,p_uuid_str))

        # If it's a subject
        elif cdm_type == 'RECORD_SUBJECT':
            # Get parameters
            entry_type = data['datum']['type']

            cmdline = data['datum']['cmdLine']
            path = data['datum']['properties']['path']

            # Sanitize special characters
            path = self.sanitize(path)

            # Check if this subject is sensitive
            if self.sensitive(entry_type,path):

                # This event is sensitive, so insert it into the database
                self.cur.execute("INSERT INTO event (uuid,type,file_uuid) " \
                                 "SELECT '{0}','{1}','{2}' " \
                                 "WHERE NOT EXISTS ( " \
                                 "SELECT uuid FROM event WHERE " \
                                 "uuid='{0}');".format(uuid_str,entry_type,path))

        # If it's a file
        elif cdm_type == 'RECORD_FILE_OBJECT':
            # Get parameters
            local_uuid = data['datum']['localPrincipal']
            l_uuid_str = str(uuid.UUID(bytes=cdm_host)) + '_' + str(uuid.UUID(bytes=local_uuid))

            filename = ''
            if 'filename' in data['datum']['baseObject']['properties']:
                filename = data['datum']['baseObject']['properties']['filename']

            # Escape special Anomaly characters
            filename = self.sanitize(filename)

            # If cache is too big, remove most unpopular cached item
            if len(self.cache.keys()) >= self.cache_max:
                smallest = sorted(d.keys(), key=lambda x: x[1], reverse=True)[0]
                del self.cache[smallest]

            # Insert filename into cache
            if uuid_str not in self.cache:
                self.cache[uuid_str] = (filename,0)

            # Add file UUID to database
            self.cur.execute("INSERT INTO file (uuid,filename) " \
                             "SELECT '{0}','{1}' " \
                             "WHERE NOT EXISTS ( " \
                             "SELECT uuid FROM file WHERE " \
                             "uuid='{0}');".format(uuid_str,filename))

    # Determine if event is sensitive
    def sensitive(self, action, obj):
        rv = False

        if action == 'EVENT_READ':
            if sensitive.sensitive_read(obj):
                rv = True
        elif action == 'EVENT_WRITE':
            if sensitive.sensitive_write(obj):
                rv = True
        elif action == 'EVENT_OPEN':
            if sensitive.sensitive_open(obj):
                rv = True
        elif action == 'EVENT_MMAP':
            if sensitive.sensitive_mmap(obj):
                rv = True
        elif action == 'SUBJECT_PROCESS':
            if sensitive.sensitive_subject(obj):
                rv = True

        return rv

    # Final flush to database
    def final_flush(self):
        self.last_time = time.time() - self.flush_time
        self.flush()

    # Flush to database
    def flush(self):
        # If no connection, return
        if self.conn is None:
            return

        # Get current time
        current = time.time()

        # If it's time to flush
        if current >= (self.last_time + self.flush_time):
            log.info('Flushing database.')

            # Commit to database
            self.conn.commit()

            # Close cursor
            self.cur.close()

            # Create cursor
            self.cur = self.conn.cursor()

            # Set new last time
            self.last_time = time.time()

    # Shutdown database connection
    def shutdown(self):
        if self.conn is not None:
            self.conn.close()
