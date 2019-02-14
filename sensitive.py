import re

# Lists of regex's to describe objects considered as sensitive

sensitive_read_list =   [ "/dev/mem", \
                          "/etc/passwd", \
                          "/etc/shadow", \
                          # Where SSH files are
                          ".*\.ssh/id_rsa", \
                          # Sensitive files for Firefox (store passwords)
                          ".*\.default/key3\.db", \
                          ".*\.default/logins\.json", \
                          ".*\.default/signons\.sqlite", \
                        ]

sensitive_write_list =  [ "/dev/mem", \
                          "/.*", \
                          "/etc/.*", \
                          "/\..*", \
                          "/usr/local/.*", \
                          "/usr/local/bin/.*", \
                          "/var/spool/cron/crontabs/root", \
                          "/bin/.*", \
                          "/boot/.*", \
                          "/var/.*", \
                          "/usr/bin/.*", \
                          "/dev/.*", \
                          "/etc/security/.*", \
                          "/usr/spool/.*", \
                          "/usr/etc/.*", \
                          "/usr/kvm/.*", \
                          "/usr/.*", \
                          "/usr/lib/.*", \
                          # Important DNS information
                          "/etc/hosts", \
                          # Sensitive files for Firefox (store passwords)
                          ".*\.default/key3\.db", \
                          ".*\.default/logins\.json", \
                          ".*\.default/signons\.sqlite", \
                          # Stores extensions for Firefox
                          ".*\.default/extensions", \
                        ]

sensitive_open_list =   [ ".*libpam.*", \
                        ]

sensitive_mmap_list =   [ ".*libpam.*", \
                        ]

sensitive_subject_list =    [ "/bin/ip", \
                              "/bin/ifconfig", \
                              "/bin/netstat", \
                              # Changes permissions of files/folders
                              "/bin/chmod", \
                              # Changes ownership of files/folders
                              "/bin/chown", \
                            ]

# Determines if this read action is sensitive
def sensitive_read(obj):
    global sensitive_read_list
    rv = False

    for r in sensitive_read_list:
        if re.match(r,obj) is not None:
            rv = True
            break

    return rv

# Determines if this write action is sensitive
def sensitive_write(obj):
    global sensitive_write_list
    rv = False

    for r in sensitive_write_list:
        if re.match(r,obj) is not None:
            rv = True
            break

    return rv

# Determines if this open action is sensitive
def sensitive_open(obj):
    global sensitive_open_list
    rv = False

    for r in sensitive_open_list:
        if re.match(r,obj) is not None:
            rv = True
            break

    return rv

# Determines if this mmap action is sensitive
def sensitive_mmap(obj):
    global sensitive_mmap_list
    rv = False

    for r in sensitive_mmap_list:
        if re.match(r,obj) is not None:
            rv = True
            break

    return rv

# Determines if this subject is sensitive
def sensitive_subject(obj):
    global sensitive_subject_list
    rv = False

    for r in sensitive_subject_list:
        if re.match(r,obj) is not None:
            rv = True
            break

    return rv
