import base64
import pymongo
from pymongo import MongoClient
import os
import json
import bson
from bson.binary import Binary

def main():
    # Step 1: Load JSON data from the file
    try:
        with open('mri.json', 'r') as json_file:
            json_data_list = json.load(json_file)
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return

    # Step 2: Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["medical-images-db"]  # DB name
    mri_collection = db["mri"]  # Collection name

    # Step 3: Process each entry in the JSON data
    for json_data in json_data_list:
        # Construct the full path to the image file
        image_path = os.path.join('mri', json_data["mri_image"])  # Adjust the folder name as needed

        # Check if the image file exists
        if not os.path.exists(image_path):
            print(f"Error: The file {image_path} does not exist.")
            continue
        
        # Check if the image file has a valid type
        file_extension = os.path.splitext(image_path)[1].lower()
        if file_extension not in ['.jpg', '.jpeg', '.png']:
            print(f"Error: The file {image_path} has an invalid file type.")
            continue

        # Encode the local image to Base64
        try:
            with open(image_path, "rb") as image_file:
                encoded_image = base64.b64encode(image_file.read()).decode()  # Read as binary and encode
        except Exception as e:
            print(f"Error encoding image {image_path}: {e}")
            continue

        # Convert the base64 string into binary data (to store as 'binData' in MongoDB)
        binary_image = Binary(base64.b64decode(encoded_image))  # Convert back to binary data

        # Prepare the data to be inserted/updated
        mri_data = {
            "mri_image": binary_image,
            "body_part": json_data["body_part"],
            "status": json_data["status"],
            "impressions": json_data["impressions"],
            "disease_type": json_data.get("disease_type", []),  # Optional
            "related_info": json_data.get("related_info", None),  # Optional
            "image_info": json_data.get("image_info", None),  # Optional
            "clinic_id": json_data["clinic_id"]
        }

        # Upsert the MRI data into MongoDB
        try:
            mri_collection.update_one(
                {"clinic_id": json_data["clinic_id"], "mri_image": json_data["mri_image"]},  # Unique identifier
                {"$set": mri_data},
                upsert=True
            )
            print(f"MRI data has been inserted or updated successfully.")
        except Exception as e:
            print(f"Error inserting or updating MRI data: {e}")

if __name__ == "__main__":
    main()