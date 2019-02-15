# THEIA Consumer
https://git.tc.bbn.com/bbn/ta3-api-bindings-python
http://www.postgresqltutorial.com/postgresql-python/insert/
https://stackoverflow.com/questions/13902337/postgresql-database-only-insert-if-the-record-doesnt-exist#13902402

sudo apt-get install python-psycopg2

## Create database
```
sudo -u postgres psql
postgres=# create database "anomaly.db";
postgres=# create user theia with encrypted password 'darpatheia1';
postgres=# grant all privileges on database "anomaly.db" to theia;

psql -d anomaly.db -f create.sql
psql -d anomaly.db -f clear.sql
```

## Run Neo4j CSV creator
```
$ python create_neo4j_csv.py handler_neo4j.cfg
```

## Run Anomaly Detector
```
$ python anomaly_detector.py handler_anomaly.cfg
```

## Access anomaly database
```
$ psql -U username -W anomaly.db
```

## List sensitive events
```
psql> SELECT e.type,f.filename FROM event AS e, file AS f WHERE f.uuid = e.file_uuid;
```

## Packaging
  1. Install FPM: https://fpm.readthedocs.io/en/latest/installing.html
  2. Run: `$ ./package.sh`

## Unpackaging
  1. Run: `$ dpkg-deb -e analysis-db-tools_1.0-0_amd64.deb .`
