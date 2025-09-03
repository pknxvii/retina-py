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
from haystack_integrations.document_stores.qdrant import QdrantDocumentStore

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


class NativeHaystackPipeline:
    """Pipeline using native Haystack converters - Singleton implementation"""
    
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
            self.logger.info("[Native Pipeline] Initializing Singleton instance")
            self._document_store = None
            self._processing_pipeline = None
            self.converters = HaystackNativeConverters()
            
            # File type mappings - simplified for PDF and TXT only
            self.file_type_map = {
                '.pdf': DocumentType.PDF,
                '.txt': DocumentType.TXT,
            }
            
            # Mark as initialized to prevent re-initialization
            NativeHaystackPipeline._initialized = True
        else:
            self.logger = structlog.get_logger()
            self.logger.debug("[Native Pipeline] Returning existing Singleton instance")
    
    @property
    def document_store(self):
        """Initialize document store lazily"""
        if self._document_store is None:
            qdrant_config = configuration["qdrant"]
            self._document_store = QdrantDocumentStore(
                url=qdrant_config["url"],
                index=qdrant_config["index"],
                embedding_dim=qdrant_config["embedding_dim"],
                recreate_index=qdrant_config["recreate_index"],
                return_embedding=qdrant_config["return_embedding"],
                wait_result_from_api=qdrant_config["wait_result_from_api"]
            )
        return self._document_store
    
    @property
    def processing_pipeline(self):
        """Initialize processing pipeline lazily"""
        if self._processing_pipeline is None:
            self.logger.info("[Native Pipeline] Creating processing pipeline (first time)")
            self._processing_pipeline = self.create_processing_pipeline()
        else:
            self.logger.debug("[Native Pipeline] Reusing existing processing pipeline")
        return self._processing_pipeline
    
    @classmethod
    def get_instance_id(cls):
        """Get the instance ID for debugging singleton behavior"""
        if cls._instance is not None:
            return id(cls._instance)
        return None
    
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
    
    def create_processing_pipeline(self) -> Pipeline:
        """Create the document processing pipeline"""
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
            document_store=self.document_store,
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
    
    def run_indexing_pipeline(self, doc_id: str, object_path: str):
        """
        Run the indexing pipeline using native Haystack converters
        
        Args:
            doc_id: Unique document identifier
            object_path: MinIO object path
        """
        self.logger.info(f"[Native Pipeline] Starting indexing for {doc_id}")
        self.logger.info(f"[Native Pipeline] Object path: {object_path}")
        
        try:
            # Step 1: Download from MinIO
            self.logger.info(f"[Native Pipeline] Downloading file...")
            file_bytes = download_file(object_path)
            self.logger.info(f"[Native Pipeline] Downloaded {len(file_bytes)} bytes")
            
            # Step 2: Save to temporary file
            file_extension = Path(object_path).suffix.lower() or '.txt'
            with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as temp_file:
                temp_file.write(file_bytes)
                temp_file_path = temp_file.name
            
            try:
                # Step 3: Convert document using native converters
                documents = self.convert_document(temp_file_path, doc_id, object_path)
                self.logger.info(f"[Native Pipeline] Converted to {len(documents)} document(s)")
                
                # Step 4: Process through pipeline
                if documents:
                    result = self.processing_pipeline.run({"cleaner": {"documents": documents}})
                    
                    documents_written = result.get("writer", {}).get("documents_written", 0)
                    self.logger.info(f"[Native Pipeline] Indexed {documents_written} chunks")
                    
                    return {
                        "status": "success",
                        "doc_id": doc_id,
                        "doc_type": self.detect_file_type(temp_file_path, object_path).value,
                        "documents_processed": len(documents),
                        "chunks_created": documents_written,
                        "converter_used": "native_haystack",
                        "message": f"Successfully indexed {documents_written} chunks using native Haystack converters"
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
            self.logger.error(f"[Native Pipeline] Error processing {doc_id}: {e}")
            raise Exception(f"Failed to index document {doc_id}: {str(e)}")

