import base64
import pymongo
from pymongo import MongoClient
import os
import json

def main():
    # Step 1: Load JSON data from the file
    try:
        with open('xray.json', 'r') as json_file:
            json_data_list = json.load(json_file)
    except Exception as e:
        print(f"Error reading JSON file: {e}")
        return

    # Step 2: Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["medicalimage_db"]
    xrays_collection = db["xrays"]

    # Step 3: Process each entry in the JSON data
    for json_data in json_data_list:
        # Construct the full path to the image file
        image_path = os.path.join('xray', json_data["xray_image"])

        # Check if the image file exists
        if not os.path.exists(image_path):
            print(f"Error: The file {image_path} does not exist.")
            continue

        # Encode the local image to Base64
        with open(image_path, "rb") as image_file:
            encoded_image = base64.b64encode(image_file.read()).decode()

        # Upsert the X-ray data into MongoDB
        try:
            xrays_collection.update_one(
                {"clinic_id": json_data["clinic_id"]},
                {"$set": {"xray_image": encoded_image, **json_data}},
                upsert=True
            )
            print(f"X-ray data for {json_data['xray_image']} inserted or updated successfully.")
        except Exception as e:
            print(f"Error inserting or updating X-ray data for {json_data['xray_image']}: {e}")

if __name__ == "__main__":
    main()