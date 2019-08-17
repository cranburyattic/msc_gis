# msc_gis
Follow the instructions in /prices/postcodes.txt

Run the following scripts to pre process the data

postcode-combine-all-postcode-files-to-single-file.py 

This combines all the postcode files into one.

postcode-fill-in-admin-code.py

This fills in any  missing Admin code with 'NA'. This ensures the load into Neo4j does not break;

postcode-reproject-spatial-data.py

This converts the postcode longitude and latitude to WGS-84

Follow the instructions in /prices/prices.txt

Run create-prices-file-for-years.sh   

This will split the file into years, and filter for property locations of interest.

Follow the instructions in /jars/jar.txt

Once the jars are downloaded, edit Dockerfile and set the correct versions of the jar.
You may also want to update the version of Neo4j that is being used.

Run build.sh to build a Docker image that now contains the jar files.

The Docker image called neo4j-algo is now ready.

Edit start-neo4j.sh

The following entries need to be updated changing $HOME/msc and $HOME/local_import_dir
to directories on your machine.  These means the data is written to you local machine
so it can be safely backed up.  The local_import_directory is where the CSVs to be loaded
into Neo4j are placed.

    --volume=$HOME/msc/data:/data \
    --volume=$HOME/msc/logs:/logs \
    --volume=$HOME/msc/conf:/conf \
    --volume=$HOME/local_import_dir:/var/lib/neo4j/import \

Run ./start-neo4j.sh

Copy postcodes_with_projections.csv to local_import_dir

In a browser goto http://localhost:7474/browser/.  You will need to set a password at this point. Each stage will be run manually to load the data

-- CREATE THE INDEXES  
CREATE INDEX ON :Station(name)  
CREATE INDEX ON :Station(location)  
CREATE INDEX ON :Postcode(location)  
CREATE INDEX ON :Postcode(postcode) 

--CREATE THE STATIONS  
USING PERIODIC COMMIT 500  
LOAD CSV WITH HEADERS FROM "file:///london-stations.csv" AS csvline  
MERGE (s:Station:Tube { name : csvline.Station, postcode : replace(csvline.Postcode," ",""), displayPostcode : csvline.Postcode, location : point({ longitude: toFloat(csvline.Longitude), latitude: toFloat(csvline.Latitude)})})

-- LINK THE STATIONS  
USING PERIODIC COMMIT 500  
LOAD CSV WITH HEADERS FROM "file:///london-tube-lines.csv" AS row
MATCH (to:Station { name : row.To}),(from:Station { name : row.From})
CREATE (from)-[:LINE { name : row.Line}]->(to)  

-- CREATE THE LINES  
USING PERIODIC COMMIT 500  
LOAD CSV WITH HEADERS FROM "file:///london-tube-lines.csv" AS row  
MATCH (to:Station { name : row.To}),(from:Station { name : row.From})  
MERGE (l:Line { name :row.Line})  
MERGE (l)<-[:IS_ON]-(to)  
MERGE (l)<-[:IS_ON]-(from)  

-- ADD DISTANCES BETWEEN STATIONS  
MATCH (from:Station)-[l:LINE]->(to:Station)  
WITH l, distance(from.location, to.location) AS d  
SET l.distance = d  

-- LOAD POSTCODES  
USING PERIODIC COMMIT 250  
LOAD CSV WITH HEADERS FROM "file:///postcodes_with_projections.csv" AS row  
CREATE (p:Postcode { postcode : replace(row.Postcode," ",""), displayPostcode :   row.Postcode, location : point({longitude : toFloat(row.longitude), latitude : toFloat  (row.latitude)})})  

-- MATCH THE POSTCODES FOR 500m  
call apoc.periodic.iterate("MATCH (p:Postcode)   
WITH p  
MATCH (l:Line)<-[:IS_ON]-(s) WHERE DISTANCE (s.location, p.location) <= 500 RETURN p,s", 
"MERGE (p)-[:IS_WITHIN { distance : 500}]->(s)",{batchSize : 5000, iterateList: true})  yield batches, total return batches, total  


-- MATCH THE POSTCODES FOR 1000m to 1500m
call apoc.periodic.iterate("MATCH (p:Postcode) 
WITH p
MATCH (l:Line)<-[:IS_ON]-(s) WHERE (DISTANCE (s.location, p.location) >= 1000 AND DISTANCE (s.location, p.location) < 1500) RETURN p,s",
"MERGE (p)-[:IS_WITHIN { distance : 500}]->(s)",{batchSize : 5000, iterateList: true}) yield batches, total return batches, total 










