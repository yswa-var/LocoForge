import os
import json
import tempfile
import time
from google_drive_ops import DriveConnector

# Path to credentials (update these paths to match your setup)
CREDENTIALS_PATH = os.path.expanduser("/Users/yash/Downloads/exercises/LocoForge/drive_config.json")
TOKEN_PATH = os.path.expanduser("/Users/yash/Downloads/exercises/LocoForge/token.json")

def pretty_print_json(title, json_str):
    """Pretty print JSON results with a title"""
    print(f"\n=== {title} ===")
    try:
        result = json.loads(json_str)
        print(json.dumps(result, indent=2))
        return result
    except json.JSONDecodeError:
        print(f"Invalid JSON: {json_str}")
        return None

def main():
    """Test all DriveConnector functionality"""
    print("Initializing Google Drive connector...")
    
    # Initialize with OAuth (will prompt for authorization if needed)
    drive = DriveConnector(
        auth_method='oauth',
        credentials_path=CREDENTIALS_PATH,
        token_path=TOKEN_PATH
    )
    
    print("Authentication successful!")
    
    # Step 1: Create a test folder
    folder_name = f"LocoForge_Test_{int(time.time())}"
    print(f"\nCreating test folder: {folder_name}")
    result = drive.create_folder(name=folder_name)
    folder_data = pretty_print_json("FOLDER CREATION RESULT", result)
    
    if not folder_data or "id" not in folder_data:
        print("Error: Could not create folder. Exiting test.")
        return
    
    folder_id = folder_data["id"]
    print(f"Created folder with ID: {folder_id}")
    
    # Step 2: Create and upload a test text file
    print("\nCreating and uploading a test text file...")
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as temp:
        temp.write("This is a test file for the LocoForge Google Drive connector.\n")
        temp.write("Line 2: Testing multi-line content.\n")
        temp.write("Line 3: The quick brown fox jumps over the lazy dog.")
        temp_file_path = temp.name
    
    result = drive.upload_file(
        file_path=temp_file_path,
        parent_folder_id=folder_id,
        name="test_document.txt"
    )
    file_data = pretty_print_json("FILE UPLOAD RESULT", result)
    
    if not file_data or "id" not in file_data:
        print("Error: Could not upload file. Exiting test.")
        os.unlink(temp_file_path)
        return
    
    file_id = file_data["id"]
    print(f"Uploaded file with ID: {file_id}")
    
    # Step 3: List files in the test folder
    print("\nListing files in the test folder...")
    result = drive.list_files(folder_id=folder_id)
    pretty_print_json("FILES IN FOLDER", result)
    
    # Step 4: Download the file
    print("\nDownloading the test file...")
    result = drive.download_file(file_id=file_id)
    file_content = pretty_print_json("DOWNLOADED FILE", result)
    
    if file_content and "content" in file_content:
        print(f"\nFile content: {file_content['content']}")
    
    # Step 5: Update the file with new content
    print("\nUpdating the test file...")
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as temp:
        temp.write("This is UPDATED content for the test file.\n")
        temp.write("The file has been modified by the DriveConnector test script.\n")
        updated_file_path = temp.name
    
    result = drive.update_file(
        file_id=file_id,
        file_path=updated_file_path,
        metadata={"description": "Updated test file"}
    )
    pretty_print_json("FILE UPDATE RESULT", result)
    
    # Step 6: Download the updated file
    print("\nDownloading the updated file...")
    result = drive.download_file(file_id=file_id)
    updated_content = pretty_print_json("UPDATED FILE CONTENT", result)
    
    # Step 7: Search for the test file
    print("\nSearching for the test file...")
    result = drive.search_files(query_text=f"name contains 'test_document'")
    pretty_print_json("SEARCH RESULTS", result)
    
    # Step 8: Try the natural language search
    print("\nTrying natural language search...")
    result = drive.search_files(query_text="text files in my test folder")
    pretty_print_json("NATURAL LANGUAGE SEARCH", result)
    
    # Step 9: Delete the test file (move to trash)
    print("\nDeleting the test file...")
    result = drive.delete_file(file_id=file_id)
    pretty_print_json("FILE DELETE RESULT", result)
    
    # Step 10: Delete the test folder (permanent deletion)
    print("\nPermanently deleting the test folder...")
    result = drive.delete_file(file_id=folder_id, permanent=True)
    pretty_print_json("FOLDER DELETE RESULT", result)
    
    # Clean up local temporary files
    print("\nCleaning up local temporary files...")
    os.unlink(temp_file_path)
    os.unlink(updated_file_path)
    
    print("\n=== TEST COMPLETE ===")
    print("All DriveConnector operations were successfully tested!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error during test: {e}")