from app.pipelines.indexing import IndexingPipelinesFactory
from app.pipelines.query import QueryPipelinesFactory
from app.storage.document_store_manager import DocumentStoreManager
from app.api.models.responses import OrganizationStats, CreateCollectionResponse


class OrganizationService:
    """Service layer for organization operations."""
    
    def __init__(self):
        pass
    
    def get_organization_stats(self) -> OrganizationStats:
        """Get statistics about active organizations in the multi-tenant pipeline."""
        # Get indexing pipeline stats
        indexing_factory = IndexingPipelinesFactory()
        indexing_stats = indexing_factory.get_organization_stats()
        
        # Get query pipeline stats
        query_factory = QueryPipelinesFactory()
        query_stats = query_factory.get_organization_stats()
        
        # Combine stats
        combined_stats = {
            **indexing_stats,
            "query_pipeline_stats": query_stats
        }
        
        return OrganizationStats(
            factory_instance_id=str(IndexingPipelinesFactory.get_instance_id()),
            multi_tenant_stats=combined_stats
        )
    
    def create_collection(self, organization_id: str) -> CreateCollectionResponse:
        """Create a Qdrant collection for an organization."""
        document_store_manager = DocumentStoreManager()
        result = document_store_manager.create_collection(organization_id)
        
        return CreateCollectionResponse(**result)
