"""
Shared Document Store Manager

This module provides a centralized way to manage QdrantDocumentStore instances
for multi-tenant applications, avoiding tight coupling between indexing and query pipelines.
"""
from typing import Dict
from haystack_integrations.document_stores.qdrant import QdrantDocumentStore
from qdrant_client import QdrantClient
from qdrant_client.http import models as rest
from app.config_loader import configuration


class DocumentStoreManager:
    """
    Centralized manager for QdrantDocumentStore instances.
    
    This class follows the Singleton pattern and provides document stores
    for different organizations while maintaining proper separation of concerns.
    """
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._document_stores: Dict[str, QdrantDocumentStore] = {}
            self._config = configuration
            DocumentStoreManager._initialized = True
    
    def get_document_store(self, organization_id: str) -> QdrantDocumentStore:
        """
        Get or create a document store for the specified organization.
        
        Args:
            organization_id: The organization identifier
            
        Returns:
            QdrantDocumentStore instance for the organization
        """
        if organization_id not in self._document_stores:
            self._document_stores[organization_id] = self._create_document_store(organization_id)
        
        return self._document_stores[organization_id]
    
    def _create_document_store(self, organization_id: str) -> QdrantDocumentStore:
        """
        Create a new QdrantDocumentStore for an organization.
        
        Args:
            organization_id: The organization identifier
            
        Returns:
            Configured QdrantDocumentStore instance
        """
        qdrant_config = self._config["qdrant"]
        tenancy_config = self._config["tenancy"]
        collection_name = f"{tenancy_config['organization_prefix']}-{organization_id}"
        
        # Check if auto collection creation is disabled
        auto_create = qdrant_config.get("auto_create_collection", True)
        
        if not auto_create:
            # Verify collection exists before creating document store
            client = QdrantClient(url=qdrant_config["url"])
            try:
                # Check if collection exists
                collections = client.get_collections()
                collection_exists = any(
                    collection.name == collection_name 
                    for collection in collections.collections
                )
                
                if not collection_exists:
                    raise ValueError(
                        f"Collection '{collection_name}' does not exist."
                    )
            except Exception as e:
                if "does not exist" in str(e) or "Collection" in str(e):
                    raise ValueError(
                        f"Collection '{collection_name}' does not exist."
                    )
                # Re-raise other exceptions
                raise
        
        return QdrantDocumentStore(
            url=qdrant_config["url"],
            index=collection_name,
            embedding_dim=qdrant_config["embedding_dim"],
            recreate_index=qdrant_config["recreate_index"],
            return_embedding=qdrant_config["return_embedding"],
            wait_result_from_api=qdrant_config["wait_result_from_api"]
        )
    
    def list_organizations(self) -> list[str]:
        """Get list of organizations with active document stores."""
        return list(self._document_stores.keys())
    
    def get_stats(self) -> dict:
        """Get statistics about active document stores."""
        return {
            "total_organizations": len(self._document_stores),
            "organizations": list(self._document_stores.keys()),
            "manager_instance_id": id(self)
        }
    
    def create_collection(self, organization_id: str) -> dict:
        """
        Create a Qdrant collection for an organization.
        
        Args:
            organization_id: The organization identifier
            
        Returns:
            Dictionary with creation status and details
        """
        tenancy_config = self._config["tenancy"]
        collection_name = f"{tenancy_config['organization_prefix']}-{organization_id}"
        
        try:
            # Get or create document store (this will create collection if needed)
            document_store = self.get_document_store(organization_id)
            
            # Check if collection exists and get info
            try:
                collection_info = document_store._client.get_collection(collection_name)
                return {
                    "success": True,
                    "message": f"Collection {collection_name} already exists",
                    "collection_name": collection_name,
                    "organization_id": organization_id,
                    "points_count": collection_info.points_count,
                    "vectors_count": collection_info.vectors_count,
                    "status": collection_info.status.name if hasattr(collection_info.status, 'name') else str(collection_info.status)
                }
            except Exception as e:
                # Collection doesn't exist, create it using direct Qdrant client
                qdrant_config = self._config["qdrant"]
                client = QdrantClient(url=qdrant_config["url"])
                
                # Create collection directly
                try:
                    client.create_collection(
                        collection_name=collection_name,
                        vectors_config=rest.VectorParams(
                            size=qdrant_config["embedding_dim"],
                            distance=rest.Distance.COSINE
                        )
                    )
                except Exception as create_error:
                    # If collection already exists, that's fine - check if it actually exists
                    if "already exists" in str(create_error):
                        # Collection exists, get its info
                        collection_info = client.get_collection(collection_name)
                        return {
                            "success": True,
                            "message": f"Collection {collection_name} already exists",
                            "collection_name": collection_name,
                            "organization_id": organization_id,
                            "points_count": collection_info.points_count,
                            "vectors_count": collection_info.vectors_count,
                            "status": collection_info.status.name if hasattr(collection_info.status, 'name') else str(collection_info.status)
                        }
                    else:
                        raise create_error
                return {
                    "success": True,
                    "message": f"Collection {collection_name} created successfully",
                    "collection_name": collection_name,
                    "organization_id": organization_id,
                    "points_count": 0,
                    "vectors_count": 0,
                    "status": "created"
                }
                
        except Exception as e:
            return {
                "success": False,
                "message": f"Failed to create collection {collection_name}: {str(e)}",
                "collection_name": collection_name,
                "organization_id": organization_id,
                "error": str(e)
            }
    
    def remove_document_store(self, organization_id: str) -> bool:
        """
        Remove document store for an organization (useful for cleanup).
        
        Args:
            organization_id: The organization identifier
            
        Returns:
            True if removed, False if not found
        """
        if organization_id in self._document_stores:
            del self._document_stores[organization_id]
            return True
        return False
