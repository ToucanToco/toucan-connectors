from laputa.common.bucket import bucket
from laputa.app.database_manager import DatabaseManager
from laputa.models.mongo_connection import init_mongo_connection

bucket.setup_config()
bucket.database_manager = DatabaseManager(bucket.config)
client = init_mongo_connection(bucket.config)
db = client['api-toucan']
