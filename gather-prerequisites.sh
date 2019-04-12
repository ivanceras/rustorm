

## Clone bazaar for the bazaar_v8 database
cd .. && git clone https://github.com/ivanceras/bazaar
cd -

## Clone sakila for sakila sample database
cd .. && git clone https://github.com/ivanceras/sakila
cd -

### Clone data sql for dota-sql sample database
cd .. && git clone https://github.com/ivanceras/dota-sql
cd -

## update and install postgresql client
apt update
apt install -y postgresql-client
