

### run data import script from dato-sql
cd ../dota-sql/data/ && sh reimport.sh
cd -

### Run data import script from bazaar
cd ../bazaar/scripts && sh setup.sh
cd -

### Copy the pre-created sqlite sakila.db for testing rustorm "with-sqlite" feature
cp ../sakila/sqlite-sakila-db/sakila.db .


### Execute the import of sakila database for postgresql
sh import-sakila-pg.sh
