# docker_postgres

1. clone the repo
2. cd into repo
3. build docker image `docker build -t my_postgres_image .`
4. run docker container `docker run -p 5432:5432 -e POSTGRES_PASSWORD=postgres --name testing -d my_postgres_image`
5. make sure you have rights to run shell scripts, if not run `chmod +x /path/to/yourscript.sh` like `chmod+x setup.sh`
6. run `./downloader.sh`
7. run `./setup.sh`
8. to set up to run, in my case once a week on Sunday at 10pm run `crontab -e` and paste the following at the bottom `0 22 * * 0 ./Desktop/docker_postgres/automate.sh`. Make sure to use your own directory.
 
