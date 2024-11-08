import json
import numpy
import vertexai
from vertexai.generative_models import GenerativeModel

region = "us-central1"
model_name = "gemini-1.5-flash-001"
prompt = """Here is a list of possible airports.
I want you to check if the names that I pass you correspond to real airports outside the US. If they do, return the original names along with their icao code, iata code, and country.
Return the results as a list of json objects: [{}, {}, {}].
Return only one answer per airport.
Don't return the records which are not airports.
Don't return any empty json objects.
Don't return an explanation for your answer.

Here are some sample runs:

I pass you:
"Neyveli Airport, India"
"Charles de Gaulle International Airport, France"
""

You return:
[{"name": "Neyveli Airport", "icao": "VONY", "iata": "NVY", "country": "India"},
{"name": "Charles de Gaulle International Airport", "icao": "LFPG", "iata": "CDG", "country": "France"}]
"""

def do_inference(airports):
    
    print("enter do_inference()")
    
    vertexai.init(location=region)
    model = GenerativeModel(model_name)
    resp = model.generate_content([airports, prompt])
    resp_text = resp.text.replace("```json", "").replace("```", "").replace("\n", "")
    print("resp_text:", resp_text)
    
    try:
        json_objs = json.loads(resp_text)
    except Exception as e:
        print("Error while parsing json:", e, ". The error was caused by:", resp_text)
        return {}
        
    return json_objs
    

def model(dbt, session):
    
    dbt.config(incremental_strategy = "insert_overwrite")
    dbt.config(unique_key = "icao")
    
    input_df = dbt.ref("int_tmp_airports_missing_icao_non_us")
        
    # since we didn't snapshot int_tmp_airports_missing_icao_non_us, 
    # use the is_incremental macro to detect if this is the first run (i.e. we're creating the table)
    # or a subsequent run (i.e. we're upserting into the existing table)
    if dbt.is_incremental:
        print("is_incremental is true")
        max_from_this = f"select max(_load_time) from {dbt.this}"
        input_df = input_df.filter(input_df._load_time >= session.sql(max_from_this).collect()[0][0])
    else:
        print("is_incremental is false")
           
    num_airports = input_df.count()
    print("num_airports:", num_airports)
    
    if num_airports == 0:
        print("Nothing to process, return an empty DataFrame")
        return session.createDataFrame(data = [], schema = ("name: string, icao: string, iata: string, country: string"))
    
    batch_size = 5
    num_batches = int(num_airports / batch_size)
    combined_results = []
    
    pandas_df = input_df.select("name", "country").sort("name").distinct().toPandas()
    batches = numpy.array_split(pandas_df, num_batches)
    
    for i in range(num_batches):
        subset_airports = batches[i].to_string(header=False)
        print("subset_airports:", subset_airports)
        results = do_inference(subset_airports)
        combined_results.extend(results)

    print("combined_results:", combined_results)
    if len(combined_results) == 0:
        print("No results, return an empty DataFrame")
        return session.createDataFrame(data = [], schema = ("name: string, icao: string, iata: string, country: string"))
    
    output_df = session.createDataFrame(combined_results)
    print("output_df:", combined_results)
     
    return output_df
