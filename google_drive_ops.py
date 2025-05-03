import os
import base64
import io
import json
from typing import Dict, List, Optional, Union, Any

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
import mimetypes

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
            if os.path.exists(self.credentials_path):
                credentials = service_account.Credentials.from_service_account_file(
                    self.credentials_path, scopes=self.SCOPES
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