from app.pipelines.indexing import HaystackPipelinesFactory
from app.api.models.responses import OrganizationStats


class OrganizationService:
    """Service layer for organization operations."""
    
    def __init__(self):
        pass
    
    def get_organization_stats(self) -> OrganizationStats:
        """Get statistics about active organizations in the multi-tenant pipeline."""
        factory = HaystackPipelinesFactory()
        stats = factory.get_organization_stats()
        
        return OrganizationStats(
            factory_instance_id=HaystackPipelinesFactory.get_instance_id(),
            multi_tenant_stats=stats
        )
