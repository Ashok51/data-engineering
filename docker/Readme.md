This is the basic pipelining practice project.
This project demonostrates the learning docker with simple pipeline, which extracts data from .csv file to the staging_customers table.
later I transformed data to the dim_customers table.
Folder strectures :
main.py contains all transformation, logs and error handling logics
db.py contains postgres url connection generation and database connection logics
data/input : contains raw input data to be extract and transformed.
sql/seed.sql : contains sql code to create tables which first runs by the docer default.
.env : enviroment variables
docker-compose.yml, Dockerfile : for docker related stuffs
