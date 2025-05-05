import os
import base64
import io
import json
import hashlib
import time
import pickle
import os.path
from cachetools import TTLCache, LRUCache
from typing import Dict, List, Optional, Union, Any, Tuple
import re
from dataclasses import dataclass
from enum import Enum
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
import mimetypes
import logging
from functools import partial
from llm_utils import process_message

logger = logging.getLogger(__name__)

class DriveCache:
    """Cache manager for Google Drive opertions"""

    def  __init__(self, cache_dir='./cache', memory_size=100, ttl=300):
        """
        Initialize the cache manager.
        
        Args:
            cache_dir: Directory for storing persistent cache
            memory_size: Maximum items in memory cache
            ttl: Time to live for cached items (seconds)
        """
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

        # In-memory cache for API responses
        self.memory_cache = TTLCache(maxsize=memory_size, ttl=ttl)
        
        # Larger LRU cache for file content
        self.content_cache = LRUCache(maxsize=20)
        
    def _get_cache_key(self, operation, params):
        """Generate a unique cache key for the operation and parameters"""
        param_str = json.dumps(params, sort_keys=True)
        key = f"{operation}:{param_str}"
        return hashlib.md5(key.encode()).hexdigest()
    
    def get(self, operation, params):
        """Get a cached result for an operation"""
        key = self._get_cache_key(operation, params)
        
        # First check memory cache
        if key in self.memory_cache:
            logger.debug(f"Cache hit (memory): {operation}")
            return self.memory_cache[key]
        
        # Then check disk cache
        cache_file = os.path.join(self.cache_dir, key)
        if os.path.exists(cache_file):
            try:
                # Check if cache is still valid (file modified time)
                if time.time() - os.path.getmtime(cache_file) <= 3600:  # 1 hour TTL for disk
                    with open(cache_file, 'rb') as f:
                        logger.debug(f"Cache hit (disk): {operation}")
                        result = pickle.load(f)
                        # Add back to memory cache
                        self.memory_cache[key] = result
                        return result
            except Exception as e:
                logger.warning(f"Error reading cache: {str(e)}")
        
        # Cache miss
        return None
    
    def set(self, operation, params, result, persist=False):
        """Store a result in cache"""
        key = self._get_cache_key(operation, params)
        
        # Always store in memory cache
        self.memory_cache[key] = result
        
        # Optionally persist to disk
        if persist:
            cache_file = os.path.join(self.cache_dir, key)
            try:
                with open(cache_file, 'wb') as f:
                    pickle.dump(result, f)
            except Exception as e:
                logger.warning(f"Error writing to cache: {str(e)}")
    
    def invalidate(self, operation=None, params=None):
        """Invalidate cache entries"""
        if operation and params:
            # Invalidate specific entry
            key = self._get_cache_key(operation, params)
            if key in self.memory_cache:
                del self.memory_cache[key]
            
            cache_file = os.path.join(self.cache_dir, key)
            if os.path.exists(cache_file):
                try:
                    os.remove(cache_file)
                except:
                    pass
        elif operation:
            # Invalidate all entries for an operation
            keys_to_remove = [k for k in list(self.memory_cache.keys()) 
                             if k.startswith(operation)]
            for k in keys_to_remove:
                del self.memory_cache[k]
        else:
            # Clear entire cache
            self.memory_cache.clear()
            
            # Remove disk cache files
            for file in os.listdir(self.cache_dir):
                try:
                    os.remove(os.path.join(self.cache_dir, file))
                except:
                    pass
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
                 max_workers: int = 10,
                 enable_cache: bool = True):
        """
        Initialize the Google Drive connector with authentication.
        
        Args:
            auth_method: Authentication method ('service_account' or 'oauth')
            credentials_path: Path to the credentials file
            token_path: Path to the token file (for OAuth)
            max_workers: Maximum number of concurrent operations
            enable_cache: Whether to enable caching
        """
        self.auth_method = auth_method
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.service = self._authenticate()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        
        self.cache_enabled = enable_cache
        if enable_cache:
            self.cache = DriveCache()
            logger.info("Drive cache initialized")

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
        params = {
        "query": query,
        "folder_id": folder_id,
        "page_size": page_size,
        "fields": fields
        }
        
        if self.cache_enabled:
            cached_result = self.cache.get("list_files", params)
            if cached_result:
                logger.info("Using cached list_files results")
                return cached_result
        
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
        
        # Return just the files array directly
        result_json =  json.dumps({"files": results.get("files", [])})
        
        if self.cache_enabled:
            self.cache.set("list_files", params, result_json)
    
        return result_json
    
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
        if self.cache_enabled:
        # Invalidate list_files cache since folder contents changed
            self.cache.invalidate("list_files")
            # If we're uploading to a specific folder, invalidate that folder's listings
            if parent_folder_id:
                self.cache.invalidate("list_files", {"folder_id": parent_folder_id})
            # Invalidate search results since new content is available
            self.cache.invalidate("search_files")
        
        
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
        
        if self.cache_enabled:
        # Invalidate list_files cache since folder structure changed
            self.cache.invalidate("list_files")
            # If we're creating in a specific folder, invalidate that folder's listing
            if parent_folder_id:
                self.cache.invalidate("list_files", {"folder_id": parent_folder_id})
        
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
            if self.cache_enabled:
                # Invalidate specific file download cache
                self.cache.invalidate("download_file", {"file_id": file_id})
                # Invalidate list operations that might show this file
                self.cache.invalidate("list_files")
                # Invalidate search results
                self.cache.invalidate("search_files")
    
        
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
        if not file_id:
            return json.dumps({"error": "No file ID provided"})
            
        try:
            # Check if file exists and get its name for better error messages
            try:
                file_info = await self._run_in_thread(
                    self.service.files().get(
                        fileId=file_id,
                        fields="id,name,trashed"
                    ).execute
                )
                
                # Check if file is already in trash when we're trying to trash it
                if not permanent and file_info.get('trashed', False):
                    return json.dumps({
                        "status": "warning", 
                        "message": f"File '{file_info.get('name', 'unknown')}' is already in trash"
                    })
                    
            except Exception as e:
                error_message = str(e)
                if "File not found" in error_message or "not found" in error_message:
                    return json.dumps({
                        "error": f"File with ID '{file_id}' not found. It may have been deleted or you don't have access."
                    })
                logger.error(f"Error checking file: {error_message}")
                return json.dumps({"error": f"Error accessing file: {error_message}"})

            # Now perform the actual delete operation
            if permanent:
                await self._run_in_thread(
                    self.service.files().delete(fileId=file_id).execute
                )
                response = {"status": "success", "message": f"File '{file_info.get('name', file_id)}' permanently deleted"}
            else:
                # Move to trash
                await self._run_in_thread(
                    self.service.files().update(
                        fileId=file_id,
                        body={"trashed": True}
                    ).execute
                )
                response = {"status": "success", "message": f"File '{file_info.get('name', file_id)}' moved to trash"}
                
                if self.cache_enabled:
                    # Invalidate the specific file's download cache
                    self.cache.invalidate("download_file", {"file_id": file_id})
                    # Invalidate list_files since directory contents changed
                    self.cache.invalidate("list_files")
                    # If we know the parent folder, specifically invalidate that cache
                    if 'parents' in file_info:
                        for parent in file_info.get('parents', []):
                            self.cache.invalidate("list_files", {"folder_id": parent})
                    # Invalidate search operations
                    self.cache.invalidate("search_files")
            
            return json.dumps(response)
                
        except Exception as e:
            error_message = str(e)
            logger.error(f"Error deleting file {file_id}: {error_message}", exc_info=True)
            
            if "insufficient permissions" in error_message.lower():
                return json.dumps({"error": "You don't have permission to delete this file"})
            else:
                return json.dumps({"error": f"Delete operation failed: {error_message}"})
    
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
                 credentials_path: str = None,
                 token_path: str = None,
                 max_workers: int = 10):
        """
        Initialize the Drive Agent with authentication.
        
        Args:
            auth_method: Authentication method ('service_account' or 'oauth')
            credentials_path: Path to the credentials file (optional, will use default paths if None)
            token_path: Path to the token file (optional, will use default paths if None)
            max_workers: Maximum number of concurrent operations
        """
        # Set default paths if not provided
        if credentials_path is None:
            credentials_path = os.path.join(os.path.dirname(__file__), 'credentials.json')
        if token_path is None:
            token_path = os.path.join(os.path.dirname(__file__), 'token.json')
            
        # Verify credentials file exists
        if not os.path.exists(credentials_path):
            raise FileNotFoundError(
                f"Credentials file not found at {credentials_path}. "
                "Please ensure you have downloaded the credentials file from Google Cloud Console."
            )
            
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
            
            result = None
            if drive_command.operation == DriveOperation.LIST:
                result = await self.drive.list_files(**drive_command.parameters)
                
            elif drive_command.operation == DriveOperation.DOWNLOAD:
                result = await self.drive.download_file(**drive_command.parameters)
                
            elif drive_command.operation == DriveOperation.UPLOAD:
                result = await self.drive.upload_file(**drive_command.parameters)
                
            elif drive_command.operation == DriveOperation.CREATE_FOLDER:
                result = await self.drive.create_folder(**drive_command.parameters)
                
            elif drive_command.operation == DriveOperation.UPDATE:
                result = await self.drive.update_file(**drive_command.parameters)
                
            elif drive_command.operation == DriveOperation.DELETE:
                try:
                    result = await self.drive.delete_file(**drive_command.parameters)
                except Exception as e:
                    if "File not found" in str(e):
                        return json.dumps({"error": "The file or folder you're trying to delete was not found. It may have been already deleted or moved."})
                    raise
                
            elif drive_command.operation == DriveOperation.SEARCH:
                result = await self.drive.search_files(**drive_command.parameters)
            
            # If result is already a JSON string, return it directly
            if isinstance(result, str):
                try:
                    # Verify it's valid JSON
                    json.loads(result)
                    return result
                except json.JSONDecodeError:
                    # If not valid JSON, wrap it in a JSON object
                    return json.dumps({"result": result})
            
            # If result is a dict or other object, convert to JSON
            return json.dumps(result)
                
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
    
    async def close(self):
        """Clean up resources used by DriveAgent."""
        if hasattr(self.drive, 'executor') and not self.drive.executor._shutdown:
            self.drive.executor.shutdown(wait=False)
            logger.info("DriveAgent resources have been released")
        
        # Reset the executor to allow future operations
        if hasattr(self.drive, 'executor') and self.drive.executor._shutdown:
            self.drive.executor = ThreadPoolExecutor(max_workers=10)
            logger.info("DriveAgent executor has been reset")