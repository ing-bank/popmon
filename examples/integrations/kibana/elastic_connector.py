from elasticsearch import Elasticsearch, helpers


class ElasticConnector:
    def __init__(self):
        self.host = "localhost"
        self.port = 9200
        self.es = None
        self.connect()

    def connect(self):
        """
        Connect to the Elasticsearch instance
        """
        self.es = Elasticsearch(f"http://{self.host}:{self.port}")
        if not self.es.ping():
            raise ConnectionError(
                f"Could not connect to Elasticsearch {self.host}:{self.port}"
            )

    def create_index(self, index_name):
        """
        Create index (delete if exists)
        """
        if self.es.indices.exists(index=index_name):
            self.es.indices.delete(index=index_name)
            request_body = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0}
            }
            self.es.indices.create(index=index_name, body=request_body, ignore=400)

    def push_to_index(self, index_name, message, i):
        """
        Push a message to the index
        """
        try:
            self.es.index(index=index_name, id=i, document=message)
        except Exception as e:
            print(f"Exception is :: {str(e)}")

    def push_bulk_to_index(self, index_name, messages):
        """
        Bulk push to index
        """
        try:
            helpers.bulk(self.es, messages, index=index_name)
        except Exception as e:
            print(f"Exception is :: {str(e)}")
