from neo4j import GraphDatabase
import os


class Neo4jProvider:
    # URI = "bolt://localhost:7687"
    # AUTH = ("neo4j", "password")

    def __init__(self):
        self.driver = GraphDatabase.driver(
            os.getenv('NEO4J_URI'), auth=(os.getenv('NEO4J_USERNAME'), os.getenv('NEO4J_PASSWORD'))
        )

    def verify_connection(self):
        self.driver.verify_connectivity()

    def close(self):
        self.driver.close()

