import os
from typing import Any, Dict
import atexit

from dotenv import load_dotenv
load_dotenv()

from pymongo import MongoClient
from pymongo.errors import PyMongoError

def tool_success(key: str, result: Any) -> Dict[str, Any]:
    """Convenience function to return a success result."""
    return {
        'status': 'success',
        key: result
    }

def tool_error(message: str) -> Dict[str, Any]:
    """Convenience function to return an error result."""
    return {
        'status': 'error',
        'error_message': message
    }

def to_python(value):
    """Convert MongoDB results to Python-friendly format."""
    from bson import ObjectId
    from datetime import datetime
    
    if isinstance(value, dict):
        return {k: to_python(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [to_python(v) for v in value]
    elif isinstance(value, ObjectId):
        return str(value)
    elif isinstance(value, datetime):
        return value.isoformat()
    else:
        return value


class MongoDBForADK:
    """
    A wrapper for querying MongoDB which returns ADK-friendly responses.
    Similar interface to Neo4jForADK but for MongoDB.
    """
    _client = None
    _db = None
    database_name = "position_management"

    def __init__(self):
        mongodb_uri = os.getenv("MONGODB_URI") or "mongodb://localhost:27017/"
        mongodb_database = os.getenv("MONGODB_DATABASE") or "position_management"
        
        self.database_name = mongodb_database
        self._client = MongoClient(mongodb_uri)
        self._db = self._client[mongodb_database]
    
    def get_client(self):
        return self._client
    
    def get_database(self):
        return self._db
    
    def close(self):
        if self._client:
            self._client.close()
    
    def send_query(self, collection_name: str, pipeline: list = None, filter_query: dict = None) -> Dict[str, Any]:
        """
        Execute a MongoDB query.
        
        Args:
            collection_name: Name of the collection to query
            pipeline: Aggregation pipeline (for complex queries including $graphLookup)
            filter_query: Simple filter query (for find operations)
        
        Returns:
            Dict with status and results
        """
        try:
            collection = self._db[collection_name]
            
            if pipeline:
                # Use aggregation pipeline (for graph-like queries)
                results = list(collection.aggregate(pipeline))
            elif filter_query:
                # Use simple find
                results = list(collection.find(filter_query))
            else:
                # Return all documents (limited)
                results = list(collection.find().limit(100))
            
            # Convert to Python-friendly format
            python_results = [to_python(doc) for doc in results]
            return tool_success("query_result", python_results)
            
        except PyMongoError as e:
            return tool_error(f"MongoDB error: {str(e)}")
        except Exception as e:
            return tool_error(f"Error: {str(e)}")
    
    def graph_lookup(self, collection_name: str, start_with: Any, connect_from: str, 
                     connect_to: str, as_field: str, max_depth: int = None) -> Dict[str, Any]:
        """
        Perform graph traversal using $graphLookup.
        
        Example: Find all positions related to a trade through hierarchies
        
        Args:
            collection_name: Collection to traverse
            start_with: Starting value (e.g., trade_id)
            connect_from: Field to connect from
            connect_to: Field to connect to
            as_field: Output field name
            max_depth: Maximum recursion depth
        """
        pipeline = [
            {
                "$graphLookup": {
                    "from": collection_name,
                    "startWith": start_with,
                    "connectFromField": connect_from,
                    "connectToField": connect_to,
                    "as": as_field,
                    "maxDepth": max_depth if max_depth else 10
                }
            }
        ]
        
        return self.send_query(collection_name, pipeline=pipeline)
    
    def get_import_directory(self):
        """Returns the import directory path for CSV files."""
        return tool_success("mongodb_import_dir", "../../data/data_files")


graphdb = MongoDBForADK()

# Register cleanup function to close database connection on exit
atexit.register(graphdb.close)
