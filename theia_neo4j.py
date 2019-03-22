import sys
import os
import logging
import time
import uuid

log = logging.getLogger(__name__)
log_format = '[%(asctime)s] [%(levelname)s]: %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format=log_format)

class TheiaNeo4j(object):

    def __init__(self, conf):
        # Initialize with config settings
        self.batch_nodes = int(conf['batch_nodes'])
        self.batch_edges = int(conf['batch_edges'])
        self.flush_time = int(conf['flush_time'])
        self.csv_dir = conf['csv_directory']
        self.archive_dir = conf['archive_directory']

        # Create directories if they don't exist already
        if not os.path.exists(self.csv_dir):
            os.makedirs(self.csv_dir)

        if not os.path.exists(self.archive_dir):
            os.makedirs(self.archive_dir)

        # Create base filenames
        self.csv_pn_base = os.path.join(self.csv_dir,'principal-node-')
        self.csv_sn_base = os.path.join(self.csv_dir,'subject-node-')
        self.csv_fn_base = os.path.join(self.csv_dir,'file-node-')
        self.csv_nn_base = os.path.join(self.csv_dir,'netflow-node-')
        self.csv_fe_base = os.path.join(self.csv_dir,'forward-edge-')
        self.csv_be_base = os.path.join(self.csv_dir,'backward-edge-')
        self.csv_sn_update_base = os.path.join(self.csv_dir,'subject-update-')
        self.csv_in_base = os.path.join(self.csv_dir,'ipc-node-')

        # Initialize counters
        self.count_pn = 0
        self.count_sn = 0
        self.count_fn = 0
        self.count_nn = 0
        self.count_fe = 0
        self.count_be = 0
        self.count_sn_update = 0
        self.count_in = 0

        # Initialize file counters
        self.file_count_pn = 0
        self.file_count_sn = 0
        self.file_count_fn = 0
        self.file_count_nn = 0
        self.file_count_fe = 0
        self.file_count_be = 0
        self.file_count_sn_update = 0
        self.file_count_in = 0

        # Create initial filenames
        self.csv_pn = '{0}{1}.csv'.format(self.csv_pn_base,self.file_count_pn)
        self.csv_sn = '{0}{1}.csv'.format(self.csv_sn_base,self.file_count_sn)
        self.csv_fn = '{0}{1}.csv'.format(self.csv_fn_base,self.file_count_fn)
        self.csv_nn = '{0}{1}.csv'.format(self.csv_nn_base,self.file_count_nn)
        self.csv_fe = '{0}{1}.csv'.format(self.csv_fe_base,self.file_count_fe)
        self.csv_be = '{0}{1}.csv'.format(self.csv_be_base,self.file_count_be)
        self.csv_sn_update = '{0}{1}.csv'.format(self.csv_sn_update_base,self.file_count_sn_update)
        self.csv_in = '{0}{1}.csv'.format(self.csv_in_base,self.file_count_in)

        # Open initial files
        self.csv_pn_file = open(self.csv_pn,'w')
        self.csv_sn_file = open(self.csv_sn,'w')
        self.csv_fn_file = open(self.csv_fn,'w')
        self.csv_nn_file = open(self.csv_nn,'w')
        self.csv_fe_file = open(self.csv_fe,'w')
        self.csv_be_file = open(self.csv_be,'w')
        self.csv_sn_update_file = open(self.csv_sn_update,'w')
        self.csv_in_file = open(self.csv_in,'w')

        # Get last time (for flushing periodically)
        self.last_time = time.time()

    # To santize string for inserting into Neo4j
    def sanitize(self, string):
        string = string.replace("\\", "\\\\")
        string = string.replace("'", "\\'")
        string = string.replace("\"", "\\\"")
        string = string.replace("\n", "\\n")

        return string

    # Rotate Principle CSV files
    def rotate_principal_node(self):
        # Increment counter
        self.count_pn += 1

        # If we need to rotate
        if self.count_pn >= self.batch_nodes:
            log.info('Rotating Principal Node CSV files')

            # Flush content of file
            self.csv_pn_file.flush()

            # Close file
            self.csv_pn_file.close()

            # Set new filename
            self.file_count_pn += 1
            self.csv_pn = '{0}{1}.csv'.format(self.csv_pn_base,self.file_count_pn)

            # Open new file
            self.csv_pn_file = open(self.csv_pn,'w')

            # Reset count
            self.count_pn = 0

    # Rotate Subject CSV files
    def rotate_subject_node(self):
        # Increment counter
        self.count_sn += 1

        # If we need to rotate
        if self.count_sn >= self.batch_nodes:
            log.info('Rotating Subject Node CSV files')

            # Flush content of file
            self.csv_sn_file.flush()

            # Close file
            self.csv_sn_file.close()

            # Set new filename
            self.file_count_sn += 1
            self.csv_sn = '{0}{1}.csv'.format(self.csv_sn_base,self.file_count_sn)

            # Open new file
            self.csv_sn_file = open(self.csv_sn,'w')

            # Reset count
            self.count_sn = 0

    # Rotate File CSV files
    def rotate_file_node(self):
        # Increment counter
        self.count_fn += 1

        # If we need to rotate
        if self.count_fn >= self.batch_nodes:
            log.info('Rotating File Node CSV files')

            # Flush content of file
            self.csv_fn_file.flush()

            # Close file
            self.csv_fn_file.close()

            # Set new filename
            self.file_count_fn += 1
            self.csv_fn = '{0}{1}.csv'.format(self.csv_fn_base,self.file_count_fn)

            # Open new file
            self.csv_fn_file = open(self.csv_fn,'w')

            # Reset count
            self.count_fn = 0

    # Rotate Netflow CSV files
    def rotate_netflow_node(self):
        # Increment counter
        self.count_nn += 1

        # If we need to rotate
        if self.count_nn >= self.batch_nodes:
            log.info('Rotating Netflow Node CSV files')

            # Flush content of file
            self.csv_nn_file.flush()

            # Close file
            self.csv_nn_file.close()

            # Set new filename
            self.file_count_nn += 1
            self.csv_nn = '{0}{1}.csv'.format(self.csv_nn_base,self.file_count_nn)

            # Open new file
            self.csv_nn_file = open(self.csv_nn,'w')

            # Reset count
            self.count_nn = 0

    # Rotate forward edge files
    def rotate_forward_edge(self):
        # Increment counter
        self.count_fe += 1

        # If we need to rotate
        if self.count_fe >= self.batch_edges:
            log.info('Rotating Forward Edge CSV files')

            # Flush content of file
            self.csv_fe_file.flush()

            # Close file
            self.csv_fe_file.close()

            # Set new filename
            self.file_count_fe += 1
            self.csv_fe = '{0}{1}.csv'.format(self.csv_fe_base,self.file_count_fe)

            # Open new file
            self.csv_fe_file = open(self.csv_fe,'w')

            # Reset count
            self.count_fe = 0

    # Rotate backward edge files
    def rotate_backward_edge(self):
        # Increment counter
        self.count_be += 1

        # If we need to rotate
        if self.count_be >= self.batch_edges:
            log.info('Rotating Backward Edge CSV files')

            # Flush content of file
            self.csv_be_file.flush()

            # Close file
            self.csv_be_file.close()

            # Set new filename
            self.file_count_be += 1
            self.csv_be = '{0}{1}.csv'.format(self.csv_be_base,self.file_count_be)

            # Open new file
            self.csv_be_file = open(self.csv_be,'w')

            # Reset count
            self.count_be = 0

    # Rotate Subject Node Update CSV files
    def rotate_subject_update_node(self):
        # Increment counter
        self.count_sn_update += 1

        # If we need to rotate
        if self.count_sn_update >= self.batch_nodes:
            log.info('Rotating Subject Node Update CSV files')

            # Flush content of file
            self.csv_sn_update_file.flush()

            # Close file
            self.csv_sn_update_file.close()

            # Set new filename
            self.file_count_sn_update += 1
            self.csv_sn_update = '{0}{1}.csv'.format(self.csv_sn_update_base,self.file_count_sn_update)

            # Open new file
            self.csv_sn_update_file = open(self.csv_sn_update,'w')

            # Reset count
            self.count_sn_update = 0

    # Rotate IPC Node CSV files
    def rotate_ipc_node(self):
        # Increment counter
        self.count_in += 1

        # If we need to rotate
        if self.count_in >= self.batch_nodes:
            log.info('Rotating IPC Node CSV files')

            # Flush content of file
            self.csv_in_file.flush()

            # Close file
            self.csv_in_file.close()

            # Set new filename
            self.file_count_in += 1
            self.csv_in = '{0}{1}.csv'.format(self.csv_in_base,self.file_count_in)

            # Open new file
            self.csv_in_file = open(self.csv_in,'w')

            # Reset count
            self.count_in = 0

    # Parse CDM data
    def parse(self, data):
        # Get ubiquitous CDM info
        cdm_type = data['type']
        cdm_uuid = data['datum']['uuid']
        cdm_host = data['hostId']
        uuid_str = str(uuid.UUID(bytes=cdm_host)) + '_' + str(uuid.UUID(bytes=cdm_uuid))


        # If it's an event
        if cdm_type == 'RECORD_EVENT':
            # Get parameters
            entry_type = data['datum']['type']

            subject_uuid = data['datum']['subject']
            s_uuid_str = str(uuid.UUID(bytes=cdm_host)) + '_' + str(uuid.UUID(bytes=subject_uuid))

            predicate_uuid = data['datum']['predicateObject']
            p_uuid_str = str(uuid.UUID(bytes=cdm_host)) + '_' + str(uuid.UUID(bytes=predicate_uuid))

            predicate2_uuid = data['datum']['predicateObject2']
            p2_uuid_str = str(uuid.UUID(bytes=cdm_host)) + '_' + str(uuid.UUID(bytes=predicate2_uuid))

            timestamp = data['datum']['timestampNanos']
            size = data['datum']['size']

            # Construct CSV line
            output = '"{0}","{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}"\n'.format('EVENT', \
                                                                                      uuid_str, \
                                                                                      entry_type, \
                                                                                      s_uuid_str, \
                                                                                      p_uuid_str, \
                                                                                      p2_uuid_str, \
                                                                                      timestamp, \
                                                                                      size, \
                                                                                      entry_type)

            # If this is a backwards edge
            if entry_type == 'EVENT_READ' or \
               entry_type == 'EVENT_READ_SOCKET_PARAMS' or \
               entry_type == 'EVENT_RECVFROM' or \
               entry_type == 'EVENT_RECVMSG':

                # Write output to CSV file
                self.csv_be_file.write(output)

                # See if we need to rotate files
                self.rotate_backward_edge()

            # Else this is a forward edge
            else:
                # Write output to CSV file
                self.csv_fe_file.write(output)

                # See if we need to rotate files
                self.rotate_forward_edge()

                # If this is an execute event, we need to update a subject
                if entry_type == 'EVENT_EXECUTE':
                    # Get cmdline property
                    cmdline = data['datum']['properties']['cmdLine']
                    cmdline = cmdline.encode('utf8').strip()

                    # Escape special Neo4j characters
                    cmdline = self.sanitize(cmdline)

                    output = '"{0}","{1}"\n'.format(s_uuid_str, \
                                                    cmdline)

                    # Write output to CSV file
                    self.csv_sn_update_file.write(output)

                    # See if we need to rotate files
                    self.rotate_subject_update_node()

        # If it's a principal
        elif cdm_type == 'RECORD_PRINCIPAL':
            userid = data['datum']['userId']

            # Construct CSV line
            output = '"{0}","{1}","{2}","{3}"\n'.format('PRINCIPAL', \
                                                        uuid_str, \
                                                        userid, \
                                                        userid)

            # Write output to CSV file
            self.csv_pn_file.write(output)

            # See if we need to rotate files
            self.rotate_principal_node()

        # If it's a subject
        elif cdm_type == 'RECORD_SUBJECT':
            # Get parameters
            entry_type = data['datum']['type']

            cid = data['datum']['cid']

            parent_uuid = data['datum']['parentSubject']
            p_uuid_str = str(uuid.UUID(bytes=cdm_host)) + '_' + str(uuid.UUID(bytes=parent_uuid))

            local_uuid = data['datum']['localPrincipal']
            l_uuid_str = str(uuid.UUID(bytes=cdm_host)) + '_' + str(uuid.UUID(bytes=local_uuid))

            timestamp = data['datum']['startTimestampNanos']

            cmdline = data['datum']['cmdLine']
            cmdline = cmdline.encode('utf8').strip()

            # Escape special Neo4j characters
            cmdline = self.sanitize(cmdline)

            # Construct CSV line
            output = '"{0}","{1}","{2}","{3}","{4}","{5}","{6}","{7}","{8}"\n'.format('SUBJECT', \
                                                                                      uuid_str, \
                                                                                      entry_type, \
                                                                                      cid, \
                                                                                      p_uuid_str, \
                                                                                      l_uuid_str, \
                                                                                      timestamp, \
                                                                                      cmdline, \
                                                                                      str(cid) + ':' + cmdline)

            # Write output to CSV file
            self.csv_sn_file.write(output)

            # See if we need to rotate files
            self.rotate_subject_node()

        # If it's a file
        elif cdm_type == 'RECORD_FILE_OBJECT':
            # Get parameters
            local_uuid = data['datum']['localPrincipal']
            l_uuid_str = str(uuid.UUID(bytes=cdm_host)) + '_' + str(uuid.UUID(bytes=local_uuid))

            filename = ''
            if 'filename' in data['datum']['baseObject']['properties']:
                filename = data['datum']['baseObject']['properties']['filename']

            # Escape special Neo4j characters
            filename = self.sanitize(filename)

            # Construct CSV line
            output = '"{0}","{1}","{2}","{3}","{4}"\n'.format('FILE', \
                                                              uuid_str, \
                                                              l_uuid_str, \
                                                              filename, \
                                                              filename)

            # Write output to CSV file
            self.csv_fn_file.write(output)

            # See if we need to rotate files
            self.rotate_file_node()

        # If it's a netflow
        elif cdm_type == 'RECORD_NET_FLOW_OBJECT':
            # Get parameters
            local_address = data['datum']['localAddress']
            local_port = data['datum']['localPort']
            remote_address = data['datum']['remoteAddress']
            remote_port = data['datum']['remotePort']

            # Construct CSV line
            output = '"{0}","{1}","{2}","{3}","{4}","{5}","{6}"\n'.format('NETFLOW', \
                                                                          uuid_str, \
                                                                          local_address, \
                                                                          local_port, \
                                                                          remote_address, \
                                                                          remote_port, \
                                                                          '{0}:{1}->{2}:{3}'.format(local_address,local_port,remote_address,remote_port))

            # Write output to CSV file
            self.csv_nn_file.write(output)

            # See if we need to rotate files
            self.rotate_netflow_node()

        # If it's an ipc
        elif cdm_type == 'RECORD_IPC_OBJECT':
            # Get parameters
            entry_type = data['datum']['type']

            # Construct CSV line
            output = '"{0}","{1}","{2}","{3}"\n'.format('IPC', \
                                                        uuid_str, \
                                                        entry_type, \
                                                        entry_type)

            # Write output to CSV file
            self.csv_in_file.write(output)

            # See if we need to rotate files
            self.rotate_ipc_node()

        # If it's a memory
        elif cdm_type == 'RECORD_MEMORY_OBJECT':
            return

        # If it's a host
        elif cdm_type == 'RECORD_HOST':
            return

        # Else
        else:
            log.error('Unknown CDM type: {0}'.format(cdm_type))

    # Final rotation of files
    def final_rotate(self):
        self.last_time = time.time() - self.flush_time
        self.rotate()

    # Rotate CSV files
    def rotate(self):
        # Get current time
        current = time.time()

        # If it's time to rotate
        if current >= (self.last_time + self.flush_time):
            log.info('Rotating Neo4j CSV files.')

            # Set all counts to maximum
            self.count_pn = self.batch_nodes
            self.count_sn = self.batch_nodes
            self.count_fn = self.batch_nodes
            self.count_nn = self.batch_nodes
            self.count_fe = self.batch_edges
            self.count_be = self.batch_edges
            self.count_sn_update = self.batch_nodes
            self.count_in = self.batch_nodes

            # Call rotations
            self.rotate_principal_node()
            self.rotate_subject_node()
            self.rotate_file_node()
            self.rotate_netflow_node()
            self.rotate_forward_edge()
            self.rotate_backward_edge()
            self.rotate_subject_update_node()
            self.rotate_ipc_node()

            # Set new last time
            self.last_time = time.time()
