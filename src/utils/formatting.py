import json
from datetime import datetime
from typing import List, Dict

def format_file_list(file_list: List[Dict]) -> str:
    """Format the file list response in a readable way."""
    if not file_list:
        return "No files found."
    
    formatted_output = "### Files in Google Drive\n\n"
    
    for file in file_list:
        # Format dates
        created_time = datetime.fromisoformat(file.get('createdTime', '').replace('Z', '+00:00'))
        modified_time = datetime.fromisoformat(file.get('modifiedTime', '').replace('Z', '+00:00'))
        
        # Format file type
        mime_type = file.get('mimeType', '')
        file_type = "📁 Folder" if mime_type == "application/vnd.google-apps.folder" else "📄 File"
        
        # Format size if available
        size = file.get('size', '')
        if size:
            size = f"({int(size)/1024:.1f} KB)"
        
        formatted_output += f"""
**{file.get('name', 'Unnamed')}** {file_type} {size}
- ID: `{file.get('id', 'N/A')}`
- Created: {created_time.strftime('%Y-%m-%d %H:%M:%S')}
- Modified: {modified_time.strftime('%Y-%m-%d %H:%M:%S')}
---
"""
    
    return formatted_output

def format_download_response(response: str) -> str:
    """Format the file download response in a readable way."""
    try:
        result = json.loads(response)
        
        # Handle error case
        if "error" in result:
            error_msg = result["error"]
            if "File not found" in error_msg:
                return "❌ Error: The requested file was not found. Please check the file ID and try again."
            return f"❌ Error: {error_msg}"
        
        # Format successful download
        metadata = result.get("metadata", {})
        content = result.get("content", "")
        format_type = result.get("format", "text")
        
        output = f"""
### File Download: {metadata.get('name', 'Unnamed')}
- Type: {metadata.get('mimeType', 'Unknown')}
- Size: {int(metadata.get('size', 0))/1024:.1f} KB

### Content:
"""
        
        if format_type == 'text':
            output += f"```\n{content}\n```"
        else:
            output += f"*Content is in {format_type} format. Use appropriate tools to decode.*"
        
        return output
    except json.JSONDecodeError:
        return f"❌ Error: Invalid response format\n{response}"

def format_folder_response(response: str) -> str:
    """Format the folder creation response in a readable way."""
    try:
        result = json.loads(response)
        
        # Handle error case
        if "error" in result:
            error_msg = result["error"]
            return f"❌ Error: {error_msg}"
        
        # Format successful folder creation
        created_time = datetime.fromisoformat(result.get('createdTime', '').replace('Z', '+00:00'))
        
        output = f"""
✅ **Folder Created Successfully**

**Folder Details:**
- Name: {result.get('name', 'Unnamed')}
- ID: `{result.get('id', 'N/A')}`
- Created: {created_time.strftime('%Y-%m-%d %H:%M:%S')}
"""
        return output
    except json.JSONDecodeError:
        return f"❌ Error: Invalid response format\n{response}"

def format_delete_response(response: str) -> str:
    """Format the delete operation response in a readable way."""
    try:
        result = json.loads(response)
        
        # Handle error case
        if "error" in result:
            error_msg = result["error"]
            if "File not found" in error_msg:
                return "❌ Error: The file or folder you're trying to delete was not found. It may have been already deleted or moved."
            return f"❌ Error: {error_msg}"
        
        # Format successful deletion
        if "status" in result and result["status"] == "success":
            return f"""
✅ **Successfully Deleted**

The file or folder has been moved to trash. You can find it in your Google Drive trash folder.
"""
        
        return f"❌ Error: Unexpected response format\n{response}"
    except json.JSONDecodeError:
        return f"❌ Error: Invalid response format\n{response}"

def format_search_response(response: str) -> str:
    """Format the search results in a readable way."""
    try:
        result = json.loads(response)
        
        # Handle error case
        if "error" in result:
            error_msg = result["error"]
            return f"❌ Error: {error_msg}"
        
        # Format successful search
        files = result.get("files", [])
        if not files:
            return "No files found matching your search criteria."
        
        formatted_output = "### Search Results\n\n"
        
        for file in files:
            # Format dates
            created_time = datetime.fromisoformat(file.get('createdTime', '').replace('Z', '+00:00'))
            modified_time = datetime.fromisoformat(file.get('modifiedTime', '').replace('Z', '+00:00'))
            
            # Format file type
            mime_type = file.get('mimeType', '')
            file_type = "📁 Folder" if mime_type == "application/vnd.google-apps.folder" else "📄 File"
            
            # Format size if available
            size = file.get('size', '')
            if size:
                size = f"({int(size)/1024:.1f} KB)"
            
            formatted_output += f"""
**{file.get('name', 'Unnamed')}** {file_type} {size}
- ID: `{file.get('id', 'N/A')}`
- Created: {created_time.strftime('%Y-%m-%d %H:%M:%S')}
- Modified: {modified_time.strftime('%Y-%m-%d %H:%M:%S')}
---
"""
        
        return formatted_output
    except json.JSONDecodeError:
        return f"❌ Error: Invalid response format\n{response}" 