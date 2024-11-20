import base64
import pymongo
from pymongo import MongoClient
import os
import json

def main():
    # Step 1: Load JSON data from the file
    try:
        with open('ct_scan.json', 'r') as json_file:
            json_data_list = json.load(json_file)
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return

    # Step 2: Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["medical-images-db"]  # DB name
    ct_scans_collection = db["ct_scans"]  # Collection name

    # Step 3: Process each entry in the JSON data
    for json_data in json_data_list:
        # Construct the full path to the image file
        image_path = os.path.join('ct_scan', json_data["ct_scan"])  # Adjust the folder name as needed

        # Check if the image file exists
        if not os.path.exists(image_path):
            print(f"Error: The file {image_path} does not exist.")
            continue

        # Encode the local image to Base64
        with open(image_path, "rb") as image_file:
            encoded_image = base64.b64encode(image_file.read()).decode()

        # Prepare the data to be inserted/updated
        ct_scan_data = {
            "ct_scan": encoded_image,
            "body_part": json_data["body_part"],
            "scan_type": json_data["scan_type"],
            "contrast": json_data.get("contrast", None),  # Optional
            "status": json_data["status"],
            "impressions": json_data["impressions"],
            "disease_type": json_data.get("disease_type", []),  # Optional
            "related_info": json_data.get("related_info", None),  # Optional
            "scan_parameters": json_data.get("scan_parameters", None),  # Optional
            "image_info": json_data.get("image_info", None),  # Optional
            "clinic_id": json_data["clinic_id"]
        }

        # Upsert the CT scan data into MongoDB
        try:
            ct_scans_collection.update_one(
                {"clinic_id": json_data["clinic_id"], "ct_scan": json_data["ct_scan"]},  # Unique identifier
                {"$set": ct_scan_data},
                upsert=True
            )
            print(f"CT scan data for {json_data['ct_scan']} inserted or updated successfully.")
        except Exception as e:
            print(f"Error inserting or updating CT scan data for {json_data['ct_scan']}: {e}")

if __name__ == "__main__":
    main()