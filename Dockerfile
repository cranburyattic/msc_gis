FROM neo4j:3.5.0
RUN chmod -R 777 /var/lib/neo4j
RUN chmod -R 777 /var/lib/neo4j/conf/neo4j.conf
COPY ./jars/graph-algorithms-algo-3.5.0.1.jar /var/lib/neo4j/plugins/graph-algorithms-algo-3.5.0.1.jar 
COPY ./jars/apoc-3.5.0.1-all.jar /var/lib/neo4j/plugins/apoc-3.5.0.1-all.jar