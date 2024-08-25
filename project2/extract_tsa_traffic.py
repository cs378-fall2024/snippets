# pip install vertexai
# pip install PyPDF2
# https://cloud.google.com/vertex-ai/generative-ai/docs/reference/python/latest/vertexai.preview.generative_models
import json, os
import time
from PyPDF2 import PdfReader, PdfWriter
from pathlib import Path
import vertexai
from vertexai.generative_models import GenerativeModel, Part
from google.cloud import storage
from google.cloud.storage import transfer_manager

project_id = "solution-workspace"
location = "us-central1"
bucket_name = "tsa-traffic"
raw_folder = "raw/"                   # files which are downloaded by download_tsa_reports.py are written into this folder
split_folder = "split/"               # input location for the extract function
llm_folder = "llm_text/"              # output location for the extract function
model_name = "gemini-1.5-flash-001"   # Gemini Flash doesn't support the schema response option
prompt = "convert the file to json format. Return the date, hour of day, airport code, airport name, city, state, checkpoint, and customer traffic."
                    
def split_documents():
    
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=raw_folder)
    
    for blob in blobs:
        
        if blob.name == raw_folder:
            continue
        
        source_filename = blob.name
        print("downloading", source_filename)
        blob.download_to_filename(source_filename)
        
        start_page = 1
        pdf_reader = PdfReader(blob.name)
        pdf_writer = PdfWriter()
    
        for page_num, page_data in enumerate(pdf_reader.pages, 1):
            pdf_writer.add_page(page_data)
            remainder = page_num % 500
            
            if (page_num % 500 == 0):
                file_name = blob.name.split(".pdf")[0].replace(raw_folder, split_folder)
                file_path = f"{file_name}_{start_page}_{page_num}.pdf"
                print("trying to write", file_path)
                                
                with open(file_path, "wb") as out:
                    pdf_writer.write(out)
                    pdf_writer = PdfWriter()
                    print("wrote local file", file_path)

                # move the start page marker   
                start_page = page_num + 1
        
        # write remaining file
        if page_num > start_page:
            file_path = f"{file_name}_{start_page}_{page_num}.pdf"
            print("trying to write last file", file_path)
        
            with open(file_path, "wb") as out:
                pdf_writer.write(out)
                print("wrote last local file", file_path)
                
    
def copy_to_GCS(local_folder, gcs_folder, file_extension):
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    directory_as_path_obj = Path(local_folder)
    file_paths = directory_as_path_obj.rglob(file_extension)
    relative_paths = [path.relative_to(local_folder) for path in file_paths]
    string_paths = [str(path) for path in relative_paths]
    print("Found {} files.".format(string_paths))
    
    results = transfer_manager.upload_many_from_filenames(bucket, string_paths, source_directory=local_folder, blob_name_prefix=gcs_folder, max_workers=5)

    for name, result in zip(string_paths, results):

        if isinstance(result, Exception):
            print("Failed to upload {} due to exception: {}".format(name, result))
        else:
            print("Uploaded {} to {}.".format(name, bucket.name))
    

def extract():
	
    vertexai.init(project=project_id, location=location)
    model = GenerativeModel(model_name)
    
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=split_folder)

    for blob in blobs:
        
        if blob.name == split_folder:
            continue
        
        # check if file has already been processed
        filename = blob.name.replace(split_folder, llm_folder).replace(".pdf", ".txt")
        
        f = Path(filename)
        if f.exists():
            print(f"{filename} already exists")
            continue
        
        print(f"extracting {blob.name}")   
        file_content = Part.from_uri(f"gs://{bucket_name}/{blob.name}", "application/pdf")
        resp = model.generate_content([file_content, prompt])
        resp_str = str(resp.candidates[0].text).replace("```json", "").replace("```", "")
        print("got resp from LLM")
                
        f = open(filename, "w")
        f.write(resp_str)
        f.close()
        print("wrote file", filename)  
  
        
if __name__ == "__main__":
    split_documents() # split pdf documents due to large size
    copy_to_GCS(split_folder, split_folder, "*.pdf") # copy split documents to GCS
    extract() # call LLM and extract attributes from documents
    copy_to_GCS(llm_folder, llm_folder, "*.txt") # copy LLM output to GCS
