# Stress testing scripts for Neo4j Change Data Capture

## Setup:
The CDCStressTest class expects certain environment variables to be set.

For example in Aura:
```
# Aura
NEO4J_USERNAME=neo4j
NEO4J_URI=neo4j+s://xxxxxx.databases.neo4j.io
NEO4J_PASSWORD=xxxxxx
```
Or localhost:
```
# localhost
NEO4J_USERNAME=neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_PASSWORD=password
```