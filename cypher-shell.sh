#docker ps --filter ancestor=neo4j-algo -q > pid.txt
file='pid.txt'
while read pid; do
cat test.cql | docker exec $pid /var/lib/neo4j/bin/cypher-shell -u $1 -p $2
done < $file
