import os
import base64
import io
import json
from typing import Dict, List, Optional, Union, Any, Tuple
import re
from dataclasses import dataclass
from enum import Enum
import asyncio
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from datetime import datetime, timedelta
import faiss
from sentence_transformers import SentenceTransformer
import PyPDF2
import docx
import pandas as pd

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
import mimetypes
import logging
from olf.llm_router import process_message
from functools import partial

logger = logging.getLogger(__name__)

@dataclass
class DocumentMetadata:
    """Metadata for a document in the semantic index."""
    file_id: str
    name: str
    mime_type: str
    created_time: str
    modified_time: str
    size: int
    content_preview: str = ""  # Make content_preview optional with default empty string
    embedding: Optional[np.ndarray] = None

class SemanticIndex:
    """Manages semantic search capabilities using FAISS and sentence transformers."""
    
    def __init__(self, model_name: str = 'all-MiniLM-L6-v2'):
        """
        Initialize the semantic index.
        
        Args:
            model_name: Name of the sentence transformer model to use
        """
        self.model = SentenceTransformer(model_name)
        self.dimension = self.model.get_sentence_embedding_dimension()
        self.index = faiss.IndexFlatL2(self.dimension)
        self.documents: List[DocumentMetadata] = []
        
    def _extract_text(self, file_content: bytes, mime_type: str) -> str:
        """
        Extract text content from various file types.
        
        Args:
            file_content: Raw file content
            mime_type: MIME type of the file
            
        Returns:
            Extracted text content
        """
        try:
            if mime_type == 'application/pdf':
                pdf_file = io.BytesIO(file_content)
                reader = PyPDF2.PdfReader(pdf_file)
                text = ""
                for page in reader.pages:
                    text += page.extract_text() + "\n"
                return text
                
            elif mime_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
                docx_file = io.BytesIO(file_content)
                doc = docx.Document(docx_file)
                return "\n".join([paragraph.text for paragraph in doc.paragraphs])
                
            elif mime_type == 'text/plain':
                return file_content.decode('utf-8')
                
            elif mime_type == 'text/csv':
                csv_file = io.BytesIO(file_content)
                df = pd.read_csv(csv_file)
                return df.to_string()
                
            else:
                return ""
                
        except Exception as e:
            logger.error(f"Error extracting text from file: {str(e)}")
            return ""
    
    async def add_document(self, metadata: DocumentMetadata, content: bytes) -> None:
        """
        Add a document to the semantic index.
        
        Args:
            metadata: Document metadata
            content: Document content
        """
        # Extract text content
        text_content = self._extract_text(content, metadata.mime_type)
        if not text_content:
            return
            
        # Create document preview
        preview_length = 500
        metadata.content_preview = text_content[:preview_length] + "..." if len(text_content) > preview_length else text_content
        
        # Generate embedding
        combined_text = f"{metadata.name} {text_content}"
        embedding = self.model.encode(combined_text)
        
        # Add to FAISS index
        self.index.add(np.array([embedding], dtype=np.float32))
        metadata.embedding = embedding
        self.documents.append(metadata)
        
    async def search(self, query: str, k: int = 5) -> List[Tuple[DocumentMetadata, float]]:
        """
        Search for documents semantically similar to the query.
        
        Args:
            query: Search query
            k: Number of results to return
            
        Returns:
            List of (document, similarity score) tuples
        """
        if not self.documents:
            return []
            
        # Generate query embedding
        query_embedding = self.model.encode(query)
        
        # Search in FAISS index
        distances, indices = self.index.search(np.array([query_embedding], dtype=np.float32), k)
        
        # Return results with metadata
        results = []
        for i, (distance, idx) in enumerate(zip(distances[0], indices[0])):
            if idx < len(self.documents):  # Valid index
                results.append((self.documents[idx], float(distance)))
                
        return results
    
    def clear(self) -> None:
        """Clear the semantic index."""
        self.index = faiss.IndexFlatL2(self.dimension)
        self.documents = []

class DriveConnector:
    """
    Google Drive connector for LocoForge that provides file operations.
    Acts as an MCP server exposing resources and tools for LLM to invoke.
    """
    
    # Define scopes needed for Google Drive API
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    def __init__(self, auth_method: str = 'service_account', 
                 credentials_path: str = 'credentials.json',
                 token_path: str = 'token.json',
                 max_workers: int = 10):
        """
        Initialize the Google Drive connector with authentication.
        
        Args:
            auth_method: Authentication method ('service_account' or 'oauth')
            credentials_path: Path to the credentials file
            token_path: Path to the token file (for OAuth)
            max_workers: Maximum number of concurrent operations
        """
        self.auth_method = auth_method
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.service = self._authenticate()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.semantic_index = SemanticIndex()
        
    async def _run_in_thread(self, func, *args, **kwargs):
        """Run a synchronous function in a thread pool."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.executor,
            partial(func, *args, **kwargs)
        )
    
    def _authenticate(self) -> Any:
        """
        Authenticate with Google Drive API using the specified method.
        
        Returns:
            Google Drive API service
        """
        credentials = None
        
        if self.auth_method == 'service_account':
            # Service account authentication
            if not os.path.exists(self.credentials_path):
                raise FileNotFoundError(
                    f"Credentials file not found at {self.credentials_path}. "
                    "Please ensure you have downloaded the service account key file from Google Cloud Console."
                )
            
            try:
                with open(self.credentials_path, 'r') as f:
                    creds_data = json.load(f)
                    required_fields = ['type', 'project_id', 'private_key_id', 'private_key', 'client_email', 'token_uri']
                    missing_fields = [field for field in required_fields if field not in creds_data]
                    
                    if missing_fields:
                        raise ValueError(
                            f"Service account credentials file is missing required fields: {', '.join(missing_fields)}. "
                            "Please ensure you have downloaded the complete service account key file from Google Cloud Console."
                        )
                
                credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_path, scopes=self.SCOPES
                )
            except json.JSONDecodeError:
                raise ValueError(
                    f"Invalid JSON in credentials file {self.credentials_path}. "
                    "Please ensure the file contains valid JSON data."
                )
            except Exception as e:
                raise ValueError(
                    f"Failed to load service account credentials: {str(e)}. "
                    "Please ensure you have downloaded the correct service account key file from Google Cloud Console."
                )
        else:  # OAuth flow
            if os.path.exists(self.token_path):
                with open(self.token_path, 'r') as token:
                    credentials = Credentials.from_authorized_user_info(
                        json.load(token), self.SCOPES)
            
            # If credentials don't exist or are invalid, run the OAuth flow
            if not credentials or not credentials.valid:
                if credentials and credentials.expired and credentials.refresh_token:
                    try:
                        credentials.refresh(Request())
                    except RefreshError:
                        # If refresh fails, re-run the flow
                        credentials = None
                        
                if not credentials:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_path, self.SCOPES)
                    credentials = flow.run_local_server(port=0)
                    
                    # Save the credentials for future use
                    with open(self.token_path, 'w') as token:
                        token.write(credentials.to_json())
        
        if not credentials:
            raise ValueError(
                f"Failed to authenticate with Google Drive using {self.auth_method}. "
                f"Check that {self.credentials_path} exists and is valid."
            )
            
        return build('drive', 'v3', credentials=credentials)
    
    async def list_files(self, query: str = None, folder_id: str = None, 
                        page_size: int = 100, fields: str = None) -> str:
        """
        List files in Google Drive, optionally filtered by query and/or folder.
        
        Args:
            query: Search query string (Drive API query format)
            folder_id: ID of the folder to list files from
            page_size: Maximum number of files to return
            fields: Fields to include in the response
            
        Returns:
            JSON string containing file metadata
        """
        # Build the query
        drive_query = []
        
        if query:
            drive_query.append(query)
            
        if folder_id:
            drive_query.append(f"'{folder_id}' in parents")
            
        # Always exclude trashed files unless explicitly requested
        if not query or 'trashed' not in query:
            drive_query.append("trashed = false")
            
        final_query = ' and '.join(drive_query) if drive_query else None
        
        # Set default fields if not specified
        if not fields:
            fields = "nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, size)"
        
        # Execute the request asynchronously
        results = await self._run_in_thread(
            self.service.files().list(
                q=final_query,
                pageSize=page_size,
                fields=fields
            ).execute
        )
        
        return json.dumps(results)
    
    async def download_file(self, file_id: str, return_format: str = 'text') -> str:
        """
        Download a file from Google Drive.
        
        Args:
            file_id: ID of the file to download
            return_format: Format to return file content ('text', 'base64', or 'bytes')
            
        Returns:
            JSON string with file metadata and content
        """
        # First get file metadata
        file_metadata = await self._run_in_thread(
            self.service.files().get(
                fileId=file_id,
                fields="name,mimeType,size"
            ).execute
        )
        
        # Create a BytesIO object for the file content
        file_content = io.BytesIO()
        
        # Download the file
        request = self.service.files().get_media(fileId=file_id)
        downloader = MediaIoBaseDownload(file_content, request)
        
        # Run download in thread pool
        async def download():
            done = False
            while not done:
                status, done = await self._run_in_thread(downloader.next_chunk)
            return file_content
        
        file_content = await download()
        file_content.seek(0)
        
        # Format content according to return_format
        content = None
        if return_format == 'text':
            try:
                content = file_content.read().decode('utf-8')
            except UnicodeDecodeError:
                # If it's not a text file, fallback to base64
                file_content.seek(0)
                content = base64.b64encode(file_content.read()).decode('utf-8')
                return_format = 'base64'
        elif return_format == 'base64':
            content = base64.b64encode(file_content.read()).decode('utf-8')
        elif return_format == 'bytes':
            content = file_content.read()
            # Convert bytes to base64 for JSON serialization
            content = base64.b64encode(content).decode('utf-8')
            return_format = 'base64'
        
        result = {
            "metadata": file_metadata,
            "content": content,
            "format": return_format
        }
        
        return json.dumps(result)
    
    async def upload_file(self, file_path: str, parent_folder_id: str = None, 
                         name: str = None, mime_type: str = None) -> str:
        """
        Upload a file to Google Drive.
        
        Args:
            file_path: Path to the file to upload
            parent_folder_id: ID of the parent folder (root if None)
            name: Name for the uploaded file (defaults to file name)
            mime_type: MIME type of the file (auto-detected if None)
            
        Returns:
            JSON string with the uploaded file metadata
        """
        if not os.path.exists(file_path):
            return json.dumps({"error": f"File not found: {file_path}"})
        
        # Auto-detect name if not provided
        if not name:
            name = os.path.basename(file_path)
            
        # Auto-detect MIME type if not provided
        if not mime_type:
            mime_type, _ = mimetypes.guess_type(file_path)
            if not mime_type:
                mime_type = 'application/octet-stream'
        
        # Prepare the file metadata
        file_metadata = {'name': name}
        if parent_folder_id:
            file_metadata['parents'] = [parent_folder_id]
        
        # Create the media file upload
        media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
        
        # Upload the file asynchronously
        file = await self._run_in_thread(
            self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id,name,mimeType,createdTime,modifiedTime,size'
            ).execute
        )
        
        return json.dumps(file)
    
    async def create_folder(self, name: str, parent_folder_id: str = None) -> str:
        """
        Create a folder in Google Drive.
        
        Args:
            name: Name of the folder
            parent_folder_id: ID of the parent folder (root if None)
            
        Returns:
            JSON string with the created folder metadata
        """
        folder_metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder'
        }
        
        if parent_folder_id:
            folder_metadata['parents'] = [parent_folder_id]
        
        folder = await self._run_in_thread(
            self.service.files().create(
                body=folder_metadata,
                fields='id,name,mimeType,createdTime'
            ).execute
        )
        
        return json.dumps(folder)
    
    async def update_file(self, file_id: str, file_path: str = None, 
                         metadata: Dict = None, mime_type: str = None) -> str:
        """
        Update a file in Google Drive (content and/or metadata).
        
        Args:
            file_id: ID of the file to update
            file_path: Path to the new file content (None to update metadata only)
            metadata: Dictionary of metadata to update
            mime_type: MIME type of the file (auto-detected if None)
            
        Returns:
            JSON string with the updated file metadata
        """
        # Default to empty dict if metadata not provided
        metadata = metadata or {}
        
        if file_path:
            # Auto-detect MIME type if not provided
            if not mime_type:
                mime_type, _ = mimetypes.guess_type(file_path)
                if not mime_type:
                    mime_type = 'application/octet-stream'
            
            media = MediaFileUpload(file_path, mimetype=mime_type, resumable=True)
            
            file = await self._run_in_thread(
                self.service.files().update(
                    fileId=file_id,
                    body=metadata,
                    media_body=media,
                    fields='id,name,mimeType,modifiedTime,size'
                ).execute
            )
        else:
            # Update metadata only
            file = await self._run_in_thread(
                self.service.files().update(
                    fileId=file_id,
                    body=metadata,
                    fields='id,name,mimeType,modifiedTime'
                ).execute
            )
        
        return json.dumps(file)
    
    async def delete_file(self, file_id: str, permanent: bool = False) -> str:
        """
        Delete a file from Google Drive.
        
        Args:
            file_id: ID of the file to delete
            permanent: If True, permanently delete; otherwise, trash
            
        Returns:
            JSON string with the operation status
        """
        try:
            # First check if the file exists
            try:
                await self._run_in_thread(
                    self.service.files().get(
                        fileId=file_id,
                        fields="id"
                    ).execute
                )
            except Exception as e:
                if "File not found" in str(e):
                    return json.dumps({"error": "The file or folder you're trying to delete was not found. It may have been already deleted or moved."})
                raise

            if permanent:
                await self._run_in_thread(
                    self.service.files().delete(
                        fileId=file_id
                    ).execute
                )
                response = {"status": "success", "message": "File permanently deleted"}
            else:
                # Move to trash (this is Google Drive's default behavior)
                await self._run_in_thread(
                    self.service.files().update(
                        fileId=file_id,
                        body={"trashed": True}
                    ).execute
                )
                response = {"status": "success", "message": "File moved to trash"}
            
            return json.dumps(response)
        except Exception as e:
            logger.error(f"Error deleting file {file_id}: {str(e)}", exc_info=True)
            return json.dumps({"error": str(e)})
    
    async def search_files(self, query_text: str, page_size: int = 100) -> str:
        """
        Search for files in Google Drive.
        
        Args:
            query_text: Search query text
            page_size: Maximum number of results to return
            
        Returns:
            JSON string with search results
        """
        # For direct name search, use simpler query
        if "name contains" in query_text.lower():
            name_part = query_text.lower().split("name contains")[1].strip().strip('"\'')
            query = f"name contains '{name_part}' and trashed=false"
        else:
            # Use fullText search for natural language queries
            query = f"fullText contains '{query_text.strip()}' and trashed=false"
            
        # Execute the search asynchronously
        results = await self._run_in_thread(
            self.service.files().list(
                q=query,
                pageSize=page_size,
                fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, size)"
            ).execute
        )
        
        return json.dumps(results)

    async def index_drive(self, folder_id: str = None) -> None:
        """
        Index all files in Drive for semantic search.
        
        Args:
            folder_id: Optional folder ID to index
        """
        # Clear existing index
        self.semantic_index.clear()
        
        # List all files
        query = f"'{folder_id}' in parents" if folder_id else None
        results = await self.list_files(query=query, page_size=1000)
        files = json.loads(results).get('files', [])
        
        # Index each file
        for file in files:
            try:
                # Skip folders and Google Docs/Sheets/Slides
                if file['mimeType'] == 'application/vnd.google-apps.folder':
                    continue
                    
                # Handle Google Docs/Sheets/Slides
                if file['mimeType'] in [
                    'application/vnd.google-apps.document',
                    'application/vnd.google-apps.spreadsheet',
                    'application/vnd.google-apps.presentation'
                ]:
                    # Create metadata with empty content preview
                    metadata = DocumentMetadata(
                        file_id=file['id'],
                        name=file['name'],
                        mime_type=file['mimeType'],
                        created_time=file['createdTime'],
                        modified_time=file['modifiedTime'],
                        size=int(file.get('size', 0)),
                        content_preview=f"Google {file['mimeType'].split('.')[-1]} file: {file['name']}"
                    )
                    # Add to semantic index with empty content
                    await self.semantic_index.add_document(metadata, b"")
                    continue
                
                # For regular files, download and process content
                content_result = await self.download_file(file['id'], return_format='bytes')
                content_data = json.loads(content_result)
                
                if 'content' in content_data:
                    # Create metadata
                    metadata = DocumentMetadata(
                        file_id=file['id'],
                        name=file['name'],
                        mime_type=file['mimeType'],
                        created_time=file['createdTime'],
                        modified_time=file['modifiedTime'],
                        size=int(file.get('size', 0))
                    )
                    
                    # Add to semantic index
                    content = base64.b64decode(content_data['content'])
                    await self.semantic_index.add_document(metadata, content)
                    
            except Exception as e:
                logger.error(f"Error indexing file {file['name']}: {str(e)}")
                continue  # Continue with next file even if one fails
                
    async def semantic_search(self, query: str, confirm_intent: bool = True) -> str:
        """
        Perform semantic search with optional intent confirmation.
        
        Args:
            query: Search query
            confirm_intent: Whether to confirm intent with LLM
            
        Returns:
            JSON string with search results and intent confirmation
        """
        # Check if index is empty
        if not self.semantic_index.documents:
            return json.dumps({
                "error": "No documents indexed. Please run index_drive() first.",
                "results": []
            })
            
        # Perform semantic search
        results = await self.semantic_index.search(query)
        
        if not results:
            return json.dumps({
                "error": "No matching documents found",
                "results": []
            })
            
        # Format results for LLM confirmation
        formatted_results = []
        for doc, score in results:
            formatted_results.append({
                "name": doc.name,
                "type": doc.mime_type,
                "created": doc.created_time,
                "modified": doc.modified_time,
                "preview": doc.content_preview,
                "similarity_score": score
            })
            
        if confirm_intent:
            # Ask LLM to confirm if results match intent
            system_message = {
                "role": "system",
                "content": "You are a helpful assistant that confirms if search results match the user's intent."
            }
            
            user_message = {
                "role": "user",
                "content": f"Query: {query}\n\nFound documents:\n{json.dumps(formatted_results, indent=2)}\n\nDo these documents match the user's intent? If not, what might they be looking for instead?"
            }
            
            confirmation = process_message([system_message, user_message])
            
            return json.dumps({
                "results": formatted_results,
                "intent_confirmation": confirmation
            })
        else:
            return json.dumps({"results": formatted_results})

class DriveOperation(Enum):
    LIST = "list"
    DOWNLOAD = "download"
    UPLOAD = "upload"
    CREATE_FOLDER = "create_folder"
    UPDATE = "update"
    DELETE = "delete"
    SEARCH = "search"

# Define function schemas for OpenAI function calling
DRIVE_FUNCTIONS = [
    {
        "name": "list_files",
        "description": "List files and folders in Google Drive",
        "parameters": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Optional search query to filter files"
                },
                "folder_id": {
                    "type": "string",
                    "description": "Optional folder ID to list files from"
                }
            }
        }
    },
    {
        "name": "download_file",
        "description": "Download a file from Google Drive",
        "parameters": {
            "type": "object",
            "properties": {
                "file_id": {
                    "type": "string",
                    "description": "ID of the file to download"
                },
                "return_format": {
                    "type": "string",
                    "enum": ["text", "base64", "bytes"],
                    "description": "Format to return file content"
                }
            },
            "required": ["file_id"]
        }
    },
    {
        "name": "upload_file",
        "description": "Upload a file to Google Drive",
        "parameters": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the file to upload"
                },
                "parent_folder_id": {
                    "type": "string",
                    "description": "Optional ID of the parent folder"
                },
                "name": {
                    "type": "string",
                    "description": "Optional name for the uploaded file"
                }
            },
            "required": ["file_path"]
        }
    },
    {
        "name": "create_folder",
        "description": "Create a new folder in Google Drive",
        "parameters": {
            "type": "object",
            "properties": {
                "name": {
                    "type": "string",
                    "description": "Name of the folder to create"
                },
                "parent_folder_id": {
                    "type": "string",
                    "description": "Optional ID of the parent folder"
                }
            },
            "required": ["name"]
        }
    },
    {
        "name": "update_file",
        "description": "Update a file's content or metadata in Google Drive",
        "parameters": {
            "type": "object",
            "properties": {
                "file_id": {
                    "type": "string",
                    "description": "ID of the file to update"
                },
                "file_path": {
                    "type": "string",
                    "description": "Optional path to new file content"
                },
                "metadata": {
                    "type": "object",
                    "description": "Optional metadata to update"
                }
            },
            "required": ["file_id"]
        }
    },
    {
        "name": "delete_file",
        "description": "Delete a file from Google Drive",
        "parameters": {
            "type": "object",
            "properties": {
                "file_id": {
                    "type": "string",
                    "description": "ID of the file to delete"
                },
                "permanent": {
                    "type": "boolean",
                    "description": "Whether to permanently delete (true) or move to trash (false)"
                }
            },
            "required": ["file_id"]
        }
    },
    {
        "name": "search_files",
        "description": "Search for files in Google Drive",
        "parameters": {
            "type": "object",
            "properties": {
                "query_text": {
                    "type": "string",
                    "description": "Search query text"
                },
                "page_size": {
                    "type": "integer",
                    "description": "Maximum number of results to return"
                }
            },
            "required": ["query_text"]
        }
    }
]

@dataclass
class DriveCommand:
    operation: DriveOperation
    parameters: Dict[str, Any]

class DriveAgent:
    """
    An agent that provides natural language interaction with Google Drive operations.
    Wraps the DriveConnector to provide a more intuitive interface.
    """
    
    def __init__(self, auth_method: str = 'service_account', 
                 credentials_path: str = 'credentials.json',
                 token_path: str = 'token.json',
                 max_workers: int = 10):
        """
        Initialize the Drive Agent with authentication.
        
        Args:
            auth_method: Authentication method ('service_account' or 'oauth')
            credentials_path: Path to the credentials file
            token_path: Path to the token file (for OAuth)
            max_workers: Maximum number of concurrent operations
        """
        self.drive = DriveConnector(auth_method, credentials_path, token_path, max_workers)
        
    def _parse_command(self, command: str) -> DriveCommand:
        """
        Parse a natural language command using LLM function calling.
        
        Args:
            command: Natural language command string
            
        Returns:
            DriveCommand object with operation and parameters
        """
        try:
            # Prepare the system message with function definitions
            system_message = {
                "role": "system",
                "content": "You are a helpful assistant that helps users interact with Google Drive. "
                          "You must respond with a function call in the following format:\n"
                          "FUNCTION: <function_name>\n"
                          "ARGUMENTS: <json_arguments>"
            }
            
            # Prepare the user message
            user_message = {
                "role": "user",
                "content": f"Available functions:\n{json.dumps(DRIVE_FUNCTIONS, indent=2)}\n\nUser command: {command}"
            }
            
            # Get LLM response
            response = process_message([system_message, user_message])
            
            # Parse the response to extract function name and arguments
            try:
                # Extract function name
                function_match = re.search(r"FUNCTION:\s*(\w+)", response)
                if not function_match:
                    raise ValueError("Could not find function name in response")
                function_name = function_match.group(1)
                
                # Extract arguments
                args_match = re.search(r"ARGUMENTS:\s*(\{.*\})", response, re.DOTALL)
                if not args_match:
                    raise ValueError("Could not find arguments in response")
                parameters = json.loads(args_match.group(1))
                
            except (json.JSONDecodeError, re.error) as e:
                raise ValueError(f"Failed to parse LLM response: {str(e)}")
            
            # Map function name to DriveOperation
            operation_map = {
                "list_files": DriveOperation.LIST,
                "download_file": DriveOperation.DOWNLOAD,
                "upload_file": DriveOperation.UPLOAD,
                "create_folder": DriveOperation.CREATE_FOLDER,
                "update_file": DriveOperation.UPDATE,
                "delete_file": DriveOperation.DELETE,
                "search_files": DriveOperation.SEARCH
            }
            
            operation = operation_map.get(function_name)
            if not operation:
                raise ValueError(f"Unknown function: {function_name}")
            
            return DriveCommand(operation, parameters)
            
        except Exception as e:
            logger.error(f"Error parsing command: {str(e)}", exc_info=True)
            raise ValueError(f"Failed to parse command: {str(e)}")
        
    async def execute_command(self, command: str) -> str:
        """
        Execute a natural language command.
        
        Args:
            command: Natural language command string
            
        Returns:
            JSON string with the operation result
        """
        try:
            logger.info(f"Executing command: {command}")
            drive_command = self._parse_command(command)
            logger.info(f"Parsed command: {drive_command}")
            
            if drive_command.operation == DriveOperation.LIST:
                return await self.drive.list_files(**drive_command.parameters)
                
            elif drive_command.operation == DriveOperation.DOWNLOAD:
                return await self.drive.download_file(**drive_command.parameters)
                
            elif drive_command.operation == DriveOperation.UPLOAD:
                return await self.drive.upload_file(**drive_command.parameters)
                
            elif drive_command.operation == DriveOperation.CREATE_FOLDER:
                return await self.drive.create_folder(**drive_command.parameters)
                
            elif drive_command.operation == DriveOperation.UPDATE:
                return await self.drive.update_file(**drive_command.parameters)
                
            elif drive_command.operation == DriveOperation.DELETE:
                try:
                    return await self.drive.delete_file(**drive_command.parameters)
                except Exception as e:
                    if "File not found" in str(e):
                        return json.dumps({"error": "The file or folder you're trying to delete was not found. It may have been already deleted or moved."})
                    raise
                
            elif drive_command.operation == DriveOperation.SEARCH:
                query_text = drive_command.parameters["query_text"]
                
                # Check if this is a simple file name search
                if query_text.startswith("name contains") or query_text.endswith((".txt", ".pdf", ".doc", ".docx", ".xls", ".xlsx")):
                    return await self.drive.search_files(query_text)
                # Check if this is a semantic search query
                elif any(keyword in command.lower() for keyword in ['find', 'search for', 'look for', 'locate']):
                    # First ensure the drive is indexed
                    if not self.drive.semantic_index.documents:
                        print("Indexing drive contents for semantic search...")
                        await self.drive.index_drive()
                    return await self.drive.semantic_search(query_text, confirm_intent=True)
                else:
                    return await self.drive.search_files(query_text)
                
        except Exception as e:
            logger.error(f"Error executing command: {str(e)}", exc_info=True)
            return json.dumps({"error": str(e)})
            
    async def execute_commands(self, commands: List[str]) -> List[str]:
        """
        Execute multiple commands in parallel.
        
        Args:
            commands: List of natural language commands
            
        Returns:
            List of JSON strings with operation results
        """
        tasks = [self.execute_command(cmd) for cmd in commands]
        return await asyncio.gather(*tasks)
            
    def get_help(self) -> str:
        """
        Get help information about available commands.
        
        Returns:
            String containing help information
        """
        help_text = """
Available commands:
1. List files: "list files" or "show documents"
2. Download file: "download file id: <file_id>"
3. Upload file: "upload file from: <file_path>"
4. Create folder: "create folder named: <folder_name>"
5. Update file: "update file id: <file_id>"
6. Delete file: "delete file id: <file_id>"
7. Search files: "search for: <query>"
        """
        return help_text

async def main():
    """Run basic tests for the Drive Agent functionality."""
    print("Initializing Drive Agent...")
    # Use OAuth authentication instead of service account
    agent = DriveAgent(
        auth_method='oauth',
        credentials_path='credentials.json',
        token_path='token.json'
    )
    
    try:
        # # Test 1: List files
        # print("\nTest 1: Listing files")
        # result = await agent.execute_command("list files")
        # print(f"List files result: {result[:200]}...")  # Print first 200 chars
        
        # # Test 2: Create a test folder
        # print("\nTest 2: Creating test folder")
        # result = await agent.execute_command("create folder named: TestFolder")
        # folder_data = json.loads(result)
        # test_folder_id = folder_data.get('id')
        # print(f"Created folder: {result}")
        
        # # Test 3: Upload a test file (if exists)
        # test_file = "test.txt"
        # if os.path.exists(test_file):
        #     print(f"\nTest 3: Uploading {test_file}")
        #     result = await agent.execute_command(f"upload file from: {test_file}")
        #     print(f"Upload result: {result}")
        
        # # Test 4: Search files
        # print("\nTest 4: Searching files")
        # result = await agent.execute_command("search for: test.txt")
        # print(f"Search result: {result[:200]}...")
        
        # Test 5: Semantic search
        print("\nTest 5: Semantic search")
        # First index the drive
        print("Indexing drive contents...")
        await agent.drive.index_drive()
        
        # Then perform semantic search
        result = await agent.execute_command("find documents about testing")
        print(f"Semantic search result: {result[:200]}...")
        
        # # Test 6: Parallel operations
        # print("\nTest 6: Parallel operations")
        # commands = [
        #     "list files",
        #     "search for: test",
        #     "create folder named: ParallelTest"
        # ]
        # results = await agent.execute_commands(commands)
        # for cmd, result in zip(commands, results):
        #     print(f"\nCommand: {cmd}")
        #     print(f"Result: {result[:200]}...")
        
        # # Cleanup: Delete test folders
        # if test_folder_id:
        #     print("\nCleaning up test folders...")
        #     result = await agent.execute_command(f"delete file id: {test_folder_id}")
        #     print(f"Cleanup result: {result}")
            
    except Exception as e:
        print(f"\nError during tests: {str(e)}")
        raise

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())