import os
from io import BytesIO, StringIO
import csv, json
import urllib3
from fastapi import FastAPI, HTTPException, File, UploadFile
from pydantic import BaseModel
from tempfile import NamedTemporaryFile

import screed
import sourmash

app = FastAPI()

class Query(BaseModel):
    signature: str #json str

def sketch_file_to_sourmash(sequence_file: str):
    """Generate a sourmash sketch from a file of sequences"""
    sketch = sourmash.MinHash(0, 21, scaled=1000)
    total_bp=0
    with screed.open(sequence_file) as records:
        for record in records:
            sketch.add_sequence(record.sequence, force=True)
            total_bp += len(record.sequence)
    print(f"generated {len(sketch)} hashes by sketching {total_bp:g} bp from '{sequence_file}'")
    ss = sourmash.SourmashSignature(sketch)
    return ss

def serialize_sig(ss): # -> bytes:
    """Serialize a sourmash sketch into bytes"""
    buf = BytesIO()
    sourmash.save_signatures([ss], buf, compression=True)
    print(f"serialized sourmash signature into {len(buf.getvalue())} bytes.")
    return buf.getvalue()

# receive file, build sourmash sketch, query mastiff
@app.post("/query_from_file/")
async def query_mastiff_from_file(file: UploadFile):
    """Testing Entrypoint: Upload a file of sequences and query mastiff"""
    with NamedTemporaryFile(delete=False) as temp:
        contents = await file.read()
        temp.write(contents)
    sig = sketch_file_to_sourmash(temp.name)
    # delete the temporary file
    os.unlink(temp.name)
    http = urllib3.PoolManager()
    try:
        buf= serialize_sig(sig)
        r = http.request('POST',
                         'https://mastiff.sourmash.bio/search',
                         body=buf,
                         headers={'Content-Type': 'application/json'})
    except urllib3.exceptions.RequestError as e:
        raise HTTPException(status_code=500, detail="Server error")

    query_results_text = r.data.decode('utf-8')
    # load results into a csv reader
    csv_reader = csv.DictReader(StringIO(query_results_text))
    results = [row for row in csv_reader]
    # dump results to json
    json_data = json.dumps(results)
    print(f"Number of matches: {len(results)}")
    return {"result": json_data}

# with data from React frontend
# query mastiff with a sourmash sketch
@app.post("/query_mastiff/")
async def query_mastiff(query: Query):
    """Query mastiff""" 
    http = urllib3.PoolManager()
    try:
        json_sig = json.loads(query.signature)
        buf= serialize_sig(json_sig)
        r = http.request('POST',
                         'https://mastiff.sourmash.bio/search',
                         body=buf,
                         headers={'Content-Type': 'application/json'})
    except urllib3.exceptions.RequestError as e:
        raise HTTPException(status_code=500, detail="Server error")

    query_results_text = r.data.decode('utf-8')
    # load results into a csv reader
    csv_reader = csv.DictReader(StringIO(query_results_text))
    results = [row for row in csv_reader]
    # dump results to json
    json_data = json.dumps(results)
    print(f"Number of matches: {len(results)}")
    return {"result": json_data}

# once working, use functions to read from either file or string json (from react)
# and only one endpoint.
# @app.post('/query_from_sig/')
# async def query_from_sig(query: QuerySignature):
# #    buf = serialize_sig(query.sig)
#     mastiff_result = await query_mastiff(sig)
#     #return {'message': 'File received'}
#     return mastiff_result 

