import os
import base64
import io
import json
from typing import Dict, List, Optional, Union, Any
import re
from dataclasses import dataclass
from enum import Enum

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
import mimetypes
import logging

logger = logging.getLogger(__name__)

class DriveConnector:
    """
    Google Drive connector for LocoForge that provides file operations.
    Acts as an MCP server exposing resources and tools for LLM to invoke.
    """
    
    # Define scopes needed for Google Drive API
    SCOPES = ['https://www.googleapis.com/auth/drive']
    
    def __init__(self, auth_method: str = 'service_account', 
                 credentials_path: str = 'credentials.json',
                 token_path: str = 'token.json'):
        """
        Initialize the Google Drive connector with authentication.
        
        Args:
            auth_method: Authentication method ('service_account' or 'oauth')
            credentials_path: Path to the credentials file
            token_path: Path to the token file (for OAuth)
        """
        self.auth_method = auth_method
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.service = self._authenticate()
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
    def list_files(self, query: str = None, folder_id: str = None, 
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
        
        # Execute the request
        results = self.service.files().list(
            q=final_query,
            pageSize=page_size,
            fields=fields
        ).execute()
        
        # Convert to JSON string
        return json.dumps(results)
    
    def download_file(self, file_id: str, return_format: str = 'text') -> str:
        """
        Download a file from Google Drive.
        
        Args:
            file_id: ID of the file to download
            return_format: Format to return file content ('text', 'base64', or 'bytes')
            
        Returns:
            JSON string with file metadata and content
        """
        # First get file metadata
        file_metadata = self.service.files().get(fileId=file_id, fields="name,mimeType,size").execute()
        
        # Create a BytesIO object for the file content
        file_content = io.BytesIO()
        
        # Download the file
        request = self.service.files().get_media(fileId=file_id)
        downloader = MediaIoBaseDownload(file_content, request)
        
        done = False
        while not done:
            status, done = downloader.next_chunk()
            
        # Reset the BytesIO position to the beginning
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
        
        # Convert to JSON string
        return json.dumps(result)
    
    def upload_file(self, file_path: str, parent_folder_id: str = None, 
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
        
        # Upload the file
        file = self.service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id,name,mimeType,createdTime,modifiedTime,size'
        ).execute()
        
        # Convert to JSON string
        return json.dumps(file)
    
    def create_folder(self, name: str, parent_folder_id: str = None) -> str:
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
        
        folder = self.service.files().create(
            body=folder_metadata,
            fields='id,name,mimeType,createdTime'
        ).execute()
        
        # Convert to JSON string
        return json.dumps(folder)
    
    def update_file(self, file_id: str, file_path: str = None, 
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
            
            file = self.service.files().update(
                fileId=file_id,
                body=metadata,
                media_body=media,
                fields='id,name,mimeType,modifiedTime,size'
            ).execute()
        else:
            # Update metadata only
            file = self.service.files().update(
                fileId=file_id,
                body=metadata,
                fields='id,name,mimeType,modifiedTime'
            ).execute()
        
        # Convert to JSON string
        return json.dumps(file)
    
    def delete_file(self, file_id: str, permanent: bool = False) -> str:
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
                self.service.files().get(fileId=file_id, fields="id").execute()
            except Exception as e:
                if "File not found" in str(e):
                    return json.dumps({"error": "The file or folder you're trying to delete was not found. It may have been already deleted or moved."})
                raise

            if permanent:
                self.service.files().delete(fileId=file_id).execute()
                response = {"status": "success", "message": "File permanently deleted"}
            else:
                # Move to trash (this is Google Drive's default behavior)
                self.service.files().update(
                    fileId=file_id,
                    body={"trashed": True}
                ).execute()
                response = {"status": "success", "message": "File moved to trash"}
            
            # Convert to JSON string
            return json.dumps(response)
        except Exception as e:
            logger.error(f"Error deleting file {file_id}: {str(e)}", exc_info=True)
            return json.dumps({"error": str(e)})
    
    def search_files(self, query_text: str, page_size: int = 100) -> str:
    # For direct name search, use simpler query
        if "name contains" in query_text.lower():
            name_part = query_text.lower().split("name contains")[1].strip().strip('"\'')
            query = f"name contains '{name_part}' and trashed=false"
        else:
            # Use fullText search for natural language queries
            query = f"fullText contains '{query_text.strip()}' and trashed=false"
            
        # Execute the search directly
        results = self.service.files().list(
            q=query,
            pageSize=page_size,
            fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, size)"
        ).execute()
        
        return json.dumps(results)

class DriveOperation(Enum):
    LIST = "list"
    DOWNLOAD = "download"
    UPLOAD = "upload"
    CREATE_FOLDER = "create_folder"
    UPDATE = "update"
    DELETE = "delete"
    SEARCH = "search"

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
                 token_path: str = 'token.json'):
        """
        Initialize the Drive Agent with authentication.
        
        Args:
            auth_method: Authentication method ('service_account' or 'oauth')
            credentials_path: Path to the credentials file
            token_path: Path to the token file (for OAuth)
        """
        self.drive = DriveConnector(auth_method, credentials_path, token_path)
        
    def _parse_command(self, command: str) -> DriveCommand:
        """
        Parse a natural language command into a structured DriveCommand.
        
        Args:
            command: Natural language command string
            
        Returns:
            DriveCommand object with operation and parameters
        """
        command = command.lower().strip()
        logger.info(f"Parsing command: {command}")
        
        # List files
        if re.match(r"^(list|show|get) (files|documents|folders)", command):
            return DriveCommand(DriveOperation.LIST, {})
            
        # Download file
        if re.match(r"^(download|get) (file|document)", command):
            file_id = re.search(r"id[:\s]+([a-zA-Z0-9_-]+)", command)
            if file_id:
                return DriveCommand(DriveOperation.DOWNLOAD, {"file_id": file_id.group(1)})
                
        # Upload file
        if re.match(r"^(upload|add|put) (file|document)", command):
            file_path = re.search(r"from[:\s]+([^\s]+)", command)
            if file_path:
                return DriveCommand(DriveOperation.UPLOAD, {"file_path": file_path.group(1)})
                
        # Create folder
        if re.match(r"^(create|make) (folder|directory)", command):
            # Extract folder name after "named:" or "called:"
            folder_name = re.search(r"(?:named|called)[:\s]+([^:]+?)(?:\s*$|\s+(?:in|with|from|to|for))", command)
            if folder_name:
                return DriveCommand(DriveOperation.CREATE_FOLDER, {"name": folder_name.group(1).strip()})
                
        # Update file
        if re.match(r"^(update|modify|change) (file|document)", command):
            file_id = re.search(r"id[:\s]+([a-zA-Z0-9_-]+)", command)
            if file_id:
                return DriveCommand(DriveOperation.UPDATE, {"file_id": file_id.group(1)})
                
        # Delete file
        if re.match(r"^(delete|remove) (file|document)", command):
            file_id = re.search(r"id[:\s]+([a-zA-Z0-9_-]+)", command)
            if file_id:
                return DriveCommand(DriveOperation.DELETE, {"file_id": file_id.group(1)})
                
        # Search files
        if re.match(r"^(search|find) (for|)", command):
            # Extract search query after "for:" or "containing:"
            query = re.search(r"(?:for|containing)[:\s]+([^:]+?)(?:\s*$|\s+(?:in|with|from|to|for))", command)
            if query:
                return DriveCommand(DriveOperation.SEARCH, {"query_text": query.group(1).strip()})
                
        logger.error(f"Could not parse command: {command}")
        raise ValueError(f"Could not parse command: {command}")
        
    def execute_command(self, command: str) -> str:
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
                return self.drive.list_files()
                
            elif drive_command.operation == DriveOperation.DOWNLOAD:
                return self.drive.download_file(drive_command.parameters["file_id"])
                
            elif drive_command.operation == DriveOperation.UPLOAD:
                return self.drive.upload_file(drive_command.parameters["file_path"])
                
            elif drive_command.operation == DriveOperation.CREATE_FOLDER:
                return self.drive.create_folder(drive_command.parameters["name"])
                
            elif drive_command.operation == DriveOperation.UPDATE:
                return self.drive.update_file(drive_command.parameters["file_id"])
                
            elif drive_command.operation == DriveOperation.DELETE:
                try:
                    return self.drive.delete_file(drive_command.parameters["file_id"])
                except Exception as e:
                    if "File not found" in str(e):
                        return json.dumps({"error": "The file or folder you're trying to delete was not found. It may have been already deleted or moved."})
                    raise
                
            elif drive_command.operation == DriveOperation.SEARCH:
                return self.drive.search_files(drive_command.parameters["query_text"])
                
        except Exception as e:
            logger.error(f"Error executing command: {str(e)}", exc_info=True)
            return json.dumps({"error": str(e)})
            
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