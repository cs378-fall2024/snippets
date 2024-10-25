import json
import numpy
import vertexai
from pyspark.sql.types import StringType
from vertexai.generative_models import GenerativeModel, SafetySetting, HarmCategory, HarmBlockThreshold
from google.cloud import bigquery

project_id = "cs378-fa2024"
region = "us-central1"
model_name = "gemini-1.5-flash-001"
prompt = """Find a match for each country based on the list of 256 countries below.
For example, if I pass you the country 'Syrian Arab Republic', map it to 'Syria'.
If there is no good match, default to null. 
Do not provide an explanation with your answer. Do not include the index of the country in your answer. 
Format your answer as a list of json objects, using the schema: [{current:<string> new:<string>}].
For example, [{"current": "Syrian Arab Republic", "new": "Syria"}, {"current": "Swaziland", "new": "Eswatini"}, {"current": "ACTIVE AERO", "new": null}]
Below is the master list of countries you should match against:
"""

def do_inference(countries):
    
    print("enter do_inference()")
    
    vertexai.init(location=region)
    model = GenerativeModel(model_name)

    safety_config = [
        SafetySetting(
            category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            threshold=HarmBlockThreshold.BLOCK_ONLY_HIGH,
        ),
    ]
    resp = model.generate_content([countries, prompt], safety_settings=safety_config)
    
    if resp == {}:
        return resp
    
    resp_text = resp.text.replace("```json", "").replace("```", "").replace("\n", "")
    print("resp_text:", resp_text)
    
    try:
        json_objs = json.loads(resp_text)
    except Exception as e:
        print("Error while parsing json:", e, ". The error was caused by:", resp_text)
        return {}
        
    return json_objs
    

def clean_results(results_raw):
    
    results_clean = []
    
    for country_dict in results_raw:
        current_clean = remove_index(country_dict["current"])
        country_dict_clean = {"current": current_clean, "new": country_dict["new"]} 
        results_clean.append(country_dict_clean)
    
    return results_clean   


def remove_index(curr_str):
    
    while True:
        if curr_str[0].isdigit():
            curr_str = curr_str[1:]
        else:
            break
    
    return curr_str.strip()
    

def model(dbt, session):
    
    bq_client = bigquery.Client()
    prompt_sql = "select name from air_travel_int.Country" # hardcoding the table should be avoided, but in this case 
                                                           # it shouldn't cause any dependency issues b/c 
                                                           # int_tmp_unmatched_airline_countries is also dependent on Country
    rows = bq_client.query_and_wait(prompt_sql)
    prompt_countries = ""

    for row in rows:
        prompt_countries += f"{row['name']}, "

    final_prompt = prompt + prompt_countries[:-2]  # lose the last comma
    print("final_prompt:", final_prompt)

    input_df = dbt.ref("tmp_airline_countries_unmatched")
    num_countries = input_df.count()
    batch_size = 5
    num_batches = int(num_countries / batch_size)
    
    pandas_df = input_df.select("country").sort("country").distinct().toPandas()
    batches = numpy.array_split(pandas_df, num_batches)
    combined_results = []
    
    for i in range(num_batches):
        subset_countries = batches[i].to_string(header=False)
        print("subset_countries:", subset_countries)
        
        results = do_inference(subset_countries)
        print("results:", results)
        
        if len(results) == 0:
            continue
        
        combined_results = combined_results + results
        
    print("combined_results:", combined_results)
    
    final_results = clean_results(combined_results)    
    print("final_results:", final_results)
    
    output_df = session.createDataFrame(final_results)
    
    return output_df