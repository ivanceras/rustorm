#!/bin/bash
set -v

cd ../ && sh ./dbscripts/setup.sh && cd -

pwd

if ! type sqlite3 > /dev/null; then
    sudo apt install -y sqlite3
fi

rm sakila.db
sqlite3 sakila.db < ../sakila/sqlite-sakila-db/sqlite-sakila-schema.sql
sqlite3 sakila.db < ../sakila/sqlite-sakila-db/sqlite-sakila-insert-data.sql
