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
    
    input_df = dbt.ref("tmp_airports_missing_icao_non_us")
    
    num_airports = input_df.count()
    batch_size = 10
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
    output_df = session.createDataFrame(combined_results)
    print("output_df:", combined_results)
     
    return output_df
