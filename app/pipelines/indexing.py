import structlog
import tempfile
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from enum import Enum

from celery.utils.log import get_task_logger
from app.storage.minio_client import download_file
from app.config_loader import configuration

# Haystack imports
from haystack import Pipeline, Document
from haystack.components.converters import (
    PyPDFToDocument,
    TextFileToDocument
)
from haystack.components.preprocessors import DocumentCleaner, DocumentSplitter
from haystack.components.embedders import SentenceTransformersDocumentEmbedder
from haystack.components.writers import DocumentWriter
from haystack.document_stores.types import DuplicatePolicy

from app.storage.document_store_manager import DocumentStoreManager

class DocumentType(Enum):
    """Supported document types with native Haystack converters"""
    PDF = "pdf"
    TXT = "txt"

class HaystackNativeConverters:
    """Wrapper for native Haystack converters - simplified for PDF and TXT only"""
    
    def __init__(self):
        # Initialize only PDF and TXT converters
        self.pdf_converter = PyPDFToDocument()
        self.text_converter = TextFileToDocument()
    
    def convert_pdf(self, file_path: str) -> List[Document]:
        """Convert PDF using PyPDFToDocument"""
        result = self.pdf_converter.run(sources=[file_path])
        return result["documents"]
    
    def convert_text(self, file_path: str) -> List[Document]:
        """Convert text file using TextFileToDocument"""
        result = self.text_converter.run(sources=[file_path])
        return result["documents"]


class IndexingPipelinesFactory:
    """Multi-tenant Factory for Haystack pipelines - Singleton implementation
    
    This factory creates and manages separate Haystack pipelines for each organization,
    enabling data isolation while sharing computational resources like ML models.
    """
    
    _instance = None
    _initialized = False
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        # Only initialize once, even if __init__ is called multiple times
        if not self._initialized:
            #self.logger = structlog.get_logger()
            self.logger = get_task_logger(__name__)
            self.logger.info("[Haystack Factory] Initializing Multi-tenant Pipelines Factory")
            
            # Multi-tenant storage: collections per organization
            self._document_stores = {}  # org_id -> QdrantDocumentStore
            self._processing_pipelines = {}  # org_id -> Pipeline
            
            self.converters = HaystackNativeConverters()
            
            # File type mappings - simplified for PDF and TXT only
            self.file_type_map = {
                '.pdf': DocumentType.PDF,
                '.txt': DocumentType.TXT,
            }
            
            # Mark as initialized to prevent re-initialization
            IndexingPipelinesFactory._initialized = True
        else:
            self.logger = get_task_logger(__name__)
            self.logger.debug("[Haystack Factory] Returning existing Multi-tenant Factory instance")
    
    def get_document_store(self, organization_id: str):
        """Get or create document store for a specific organization"""
        # Delegate to the shared DocumentStoreManager for consistency
        store_manager = DocumentStoreManager()
        
        self.logger.info(f"[Haystack Factory] Getting document store for org: {organization_id}")
        return store_manager.get_document_store(organization_id)
    
    def get_processing_pipeline(self, organization_id: str):
        """Get or create processing pipeline for a specific organization"""
        if organization_id not in self._processing_pipelines:
            self.logger.info(f"[Haystack Factory] Creating processing pipeline for org: {organization_id}")
            document_store = self.get_document_store(organization_id)
            self._processing_pipelines[organization_id] = self.create_processing_pipeline(document_store)
        else:
            self.logger.info(f"[Haystack Factory] Reusing existing processing pipeline with id: {id(self._processing_pipelines[organization_id])} for org: {organization_id}")
        return self._processing_pipelines[organization_id]
    
    @classmethod
    def get_instance_id(cls):
        """Get the instance ID for debugging singleton behavior"""
        if cls._instance is not None:
            return id(cls._instance)
        return None
    
    def get_organization_stats(self):
        """Get statistics about active organizations"""
        store_manager = DocumentStoreManager()
        store_stats = store_manager.get_stats()
        
        return {
            "total_organizations": store_stats["total_organizations"],
            "organizations": store_stats["organizations"],
            "active_pipelines": len(self._processing_pipelines),
            "document_store_manager_id": store_stats["manager_instance_id"]
        }
    
    def detect_file_type(self, file_path: str, object_path: str) -> DocumentType:
        """Detect file type from extension"""
        # Try by file extension
        file_ext = Path(file_path).suffix.lower()
        if file_ext in self.file_type_map:
            return self.file_type_map[file_ext]
        
        # Try by object path extension
        object_ext = Path(object_path).suffix.lower()
        if object_ext in self.file_type_map:
            return self.file_type_map[object_ext]
        
        # Default to TXT for unknown types
        self.logger.warning(f"Unknown file type for {file_path}, defaulting to TXT")
        return DocumentType.TXT
    
    def convert_document(self, file_path: str, doc_id: str, object_path: str) -> List[Document]:
        """Convert document using appropriate native Haystack converter"""
        doc_type = self.detect_file_type(file_path, object_path)
        
        # Base metadata for all documents
        base_meta = {
            "doc_id": doc_id,
            "object_path": object_path,
            "doc_type": doc_type.value,
            "source_file": object_path.split("/")[-1],
            "file_size": os.path.getsize(file_path)
        }
        
        self.logger.info(f"Converting document {doc_id} of type {doc_type.value}")
        
        try:
            # Use native Haystack converters - simplified for PDF and TXT only
            if doc_type == DocumentType.PDF:
                documents = self.converters.convert_pdf(file_path)
            elif doc_type == DocumentType.TXT:
                documents = self.converters.convert_text(file_path)
            else:
                # Fallback to text for unknown types
                documents = self.converters.convert_text(file_path)
            # Update metadata for all documents
            for doc in documents:
                if not hasattr(doc, 'meta') or doc.meta is None:
                    doc.meta = {}
                doc.meta.update(base_meta)
                if not doc.meta.get("doc_id"):
                    doc.meta["doc_id"] = doc_id
            
            return documents
            
        except Exception as e:
            self.logger.error(f"Error converting document {doc_id}: {e}")
            return [Document(
                content=f"Error processing {doc_type.value} file: {str(e)}", 
                meta=base_meta
            )]
    
    def create_processing_pipeline(self, document_store) -> Pipeline:
        """Create the document processing pipeline for a specific document store"""
        haystack_config = configuration["haystack"]
        
        document_cleaner = DocumentCleaner(
            remove_empty_lines=haystack_config["cleaner"]["remove_empty_lines"],
            remove_extra_whitespaces=haystack_config["cleaner"]["remove_extra_whitespaces"],
            remove_repeated_substrings=haystack_config["cleaner"]["remove_repeated_substrings"]
        )
        
        document_splitter = DocumentSplitter(
            split_by=haystack_config["splitter"]["split_by"],
            split_length=haystack_config["splitter"]["split_length"],
            split_overlap=haystack_config["splitter"]["split_overlap"]
        )
        
        embedder = SentenceTransformersDocumentEmbedder(
            model=haystack_config["embedder"]["model"],
            progress_bar=haystack_config["embedder"]["progress_bar"]
        )
        
        writer = DocumentWriter(
            document_store=document_store,
            policy=DuplicatePolicy.OVERWRITE
        )
        
        # Build pipeline
        pipeline = Pipeline()
        pipeline.add_component("cleaner", document_cleaner)
        pipeline.add_component("splitter", document_splitter)
        pipeline.add_component("embedder", embedder)
        pipeline.add_component("writer", writer)
        
        # Connect components
        pipeline.connect("cleaner", "splitter")
        pipeline.connect("splitter", "embedder")
        pipeline.connect("embedder", "writer")
        
        return pipeline
    
    def run_indexing_pipeline(self, doc_id: str, object_path: str, user_id: str, organization_id: str):
        """
        Run the indexing pipeline using native Haystack converters
        
        Args:
            doc_id: Unique document identifier
            object_path: MinIO object path
            user_id: User ID
            organization_id: Organization ID
        """
        if not organization_id:
            raise ValueError("organization_id is required for multi-tenant indexing")
            
        self.logger.info(f"[Haystack Factory] Starting indexing for {doc_id} (org: {organization_id}, user: {user_id})")
        self.logger.info(f"[Haystack Factory] Object path: {object_path}")
        
        try:
            # Step 1: Download from MinIO
            self.logger.info(f"[Haystack Factory] Downloading file...")
            file_bytes = download_file(object_path)
            self.logger.info(f"[Haystack Factory] Downloaded {len(file_bytes)} bytes")
            
            #TODO: Can we run pipeline with file_bytes instead of temp file?
            # Step 2: Save to temporary file
            file_extension = Path(object_path).suffix.lower() or '.txt'
            with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as temp_file:
                temp_file.write(file_bytes)
                temp_file_path = temp_file.name
            
            try:
                # Step 3: Convert document using native converters
                documents = self.convert_document(temp_file_path, doc_id, object_path)
                self.logger.info(f"[Haystack Factory] Converted to {len(documents)} document(s)")
                
                # Step 4: Process through organization-specific pipeline
                if documents:
                    # Add tenant context to document metadata
                    for doc in documents:
                        if not hasattr(doc, 'meta') or doc.meta is None:
                            doc.meta = {}
                        doc.meta.update({
                            "user_id": user_id,
                            "organization_id": organization_id
                        })
                    
                    self.logger.info(f"[Haystack Factory] Using processing pipeline for org: {organization_id}")
                    pipeline = self.get_processing_pipeline(organization_id)
                    result = pipeline.run({"cleaner": {"documents": documents}})
                    
                    documents_written = result.get("writer", {}).get("documents_written", 0)
                    self.logger.info(f"[Haystack Factory] Indexed {documents_written} chunks")
                    
                    tenancy_config = configuration["tenancy"]
                    collection_name = f"{tenancy_config['organization_prefix']}-{organization_id}"
                    
                    return {
                        "status": "success",
                        "doc_id": doc_id,
                        "organization_id": organization_id,
                        "user_id": user_id,
                        "doc_type": self.detect_file_type(temp_file_path, object_path).value,
                        "documents_processed": len(documents),
                        "chunks_created": documents_written,
                        "collection": collection_name,
                        "message": f"Successfully indexed {documents_written} chunks for organization {organization_id}"
                    }
                else:
                    return {
                        "status": "error",
                        "doc_id": doc_id,
                        "message": "No documents were extracted"
                    }
                    
            finally:
                # Clean up
                if os.path.exists(temp_file_path):
                    os.unlink(temp_file_path)
                    
        except Exception as e:
            self.logger.error(f"[Haystack Factory] Error processing {doc_id}: {e}")
            raise Exception(f"Failed to index document {doc_id}: {str(e)}")

