
## set the `localhost` to `postgres` databse url in dota-sql database scripts
cd ../dota-sql/
find . -name '*.sh' -type f -exec sed -i 's/localhost/postgres/g' {} +
cd -

## set the `localhost` to `postgres` databse url in bazaar database scripts
cd ../bazaar/
find . -name '*.sh' -type f -exec sed -i 's/localhost/postgres/g' {} +
cd -

## set the `localhost` to `postgres` databse url in script files in rustorm project
find . -name '*.sh' -type f -exec sed -i 's/localhost/postgres/g' {} +

## set the `localhost` to `postgres` databse url in rustorm project files
find . -name '*.rs' -type f -exec sed -i 's/localhost/postgres/g' {} +



