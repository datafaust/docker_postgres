FROM library/postgres
COPY init.sql /docker-entrypoint-initdb.d/
COPY init_agg_functions.sql /docker-entrypoint-initdb.d/
EXPOSE 5432
