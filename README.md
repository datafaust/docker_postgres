# docker_postgres

1. clone the repo
2. cd into repo
3. build docker image `docker build -t my_posrtgres_image .`
4. run docker container `docker run -p 5432:5432 -e POSTGRES_PASSWORD=postgres --name testing -d my_postgres_image`
5. Next steps coming soon . . . 
