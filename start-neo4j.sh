docker run \
    --memory=8g \
    --publish=7474:7474 --publish=7687:7687 --publish=5005:5005 \
    --volume=$HOME/msc/data:/data \
    --volume=$HOME/msc/logs:/logs \
    --volume=$HOME/msc/conf:/conf \
    --volume=$HOME/local_import_dir:/var/lib/neo4j/import \
    --user 1000:1000 \
    neo4j-algo
