"""
Shared Document Store Manager

This module provides a centralized way to manage QdrantDocumentStore instances
for multi-tenant applications, avoiding tight coupling between indexing and query pipelines.
"""
from typing import Dict
from haystack_integrations.document_stores.qdrant import QdrantDocumentStore
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
