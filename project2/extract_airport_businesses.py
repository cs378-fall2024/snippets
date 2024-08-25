# pip install vertexai
# https://cloud.google.com/vertex-ai/generative-ai/docs/reference/python/latest/vertexai.preview.generative_models
import csv
import vertexai
from vertexai.generative_models import GenerativeModel, Part
from pathlib import Path
from google.cloud import storage
from google.cloud.storage import transfer_manager

project_id = "cs378-fa2024"
region = "us-central1"
bucket_name = "air-travel-data"
gcs_in_folder = "raw/airport-maps/in"
local_folder = "out-csv"
gcs_out_folder = "raw/airport-maps/out/"
model_name = "gemini-1.5-flash-001"
prompt = "what are all the businesses shown on this airport map? Be specific, extract the name of the business and assign it one or more categories. If it's a dining place, also return its top 3 menu items. Also, include which gates or other airport landmarks are nearest to the business. Return the output as json with the schema business:string, category:string, menu_items:list<string>, location:string. Do not include any other fields."

def main():
	
    vertexai.init(project=project_id, location=region)
    model = GenerativeModel(model_name)
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=gcs_in_folder)
    records = []

    for index, blob in enumerate(blobs):
        print(blob.name)
        
        airport_code = blob.name.split("/")[3].split("-")[0]
        terminal = blob.name.split("/")[3].split("-")[2].split(".")[0]
        print(f"airport_code: {airport_code}")
        print(f"terminal: {terminal}")
        
        # output file
        csvfile = open(f"{local_folder}/{airport_code}-{terminal}.csv", "w", newline="\n")
        writer = csv.writer(csvfile, delimiter="\t", quotechar="\"", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["airport_code", "terminal", "business", "category", "location", "menu_items"])
        
        image_file = Part.from_uri(f"gs://{bucket_name}/{blob.name}", "application/pdf")
        resp = model.generate_content([image_file, prompt])
        resp_text = resp.text.replace("```json", "").replace("```", "").replace("\n", "")
        payload_list = resp_text.split("},")
        
        for payload in payload_list:
            print("payload:", payload)
            
            # business
            business_start = payload.find("business\":") 
            business_end = payload.find(",", business_start)
            business = payload[business_start:business_end].replace("business", "").replace(":", "").replace("\"", "").strip()
            #print("business:", business)
            
            # category
            category_start = payload.find("category\":") 
            category_end = payload.find(",", category_start)
            category = payload[category_start:category_end].replace("category", "").replace(":", "").replace("\"", "").strip()
            #print("category:", category)
            
            # menu items
            menu_items_start = payload.find("menu_items\":") 
            menu_items_end = payload.find("],", menu_items_start)
            menu_items = payload[menu_items_start:menu_items_end].replace("menu_items", "").replace(":", "").replace("\"", "").replace("[", "").replace(",      ", ",").strip()
            
            if menu_items == None:
                menu_items = ''
            #print("menu_items:", menu_items)
            
            # location
            location_start = payload.find("location\":") 
            location_end = payload.find(",", location_start)
            location = payload[location_start:location_end].replace("location", "").replace(":", "").replace("\"", "").replace("}", "").strip()
            #print("location:", location)
            
            writer.writerow([airport_code, terminal, business, category, location, menu_items])
                                
        csvfile.close()

    
def copy_to_GCS():
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    directory_as_path_obj = Path(local_folder)
    file_paths = directory_as_path_obj.rglob("*.csv")
    relative_paths = [path.relative_to(local_folder) for path in file_paths]
    string_paths = [str(path) for path in relative_paths]
    print("Found {} files.".format(string_paths))
    
    results = transfer_manager.upload_many_from_filenames(bucket, string_paths, source_directory=local_folder, blob_name_prefix=gcs_out_folder, max_workers=5)

    for name, result in zip(string_paths, results):

        if isinstance(result, Exception):
            print("Failed to upload {} due to exception: {}".format(name, result))
        else:
            print("Uploaded {} to {}.".format(name, bucket.name))

     
if __name__ == "__main__":
    main()
    copy_to_GCS()