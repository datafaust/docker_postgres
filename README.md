# docker_postgres

The purpose of this project was to build an automated postgres docker container holding emergency management data from NYC's open data portal https://data.cityofnewyork.us/Public-Safety/EMS-Incident-Dispatch-Data/76xm-jjuj.
With coronavirus cases climbing in NYC, as a resident of NYC and a data guy I thought it be good to track data and have a basic datawarehouse set up to update periodically as data gets loaded into open data. If you wish to set this up for yourself, follow the steps below.

1. make sure you installed docker on your ubuntu machine
2. clone the repo
3. cd into repo
4. build docker image `docker build -t my_postgres_image .`
5. run docker container `docker run -p 5432:5432 -e POSTGRES_PASSWORD=postgres --name testing -d my_postgres_image`
6. make sure you have rights to run shell scripts, if not run `chmod +x /path/to/yourscript.sh` like `chmod+x setup.sh`
7. run `./downloader.sh`
8. run `./setup.sh`
9. to set up to run, in my case once a week on Sunday at 10pm run `crontab -e` and paste the following at the bottom `0 22 * * 0 ./Desktop/docker_postgres/automate.sh`. Make sure to use your own directory.
 
