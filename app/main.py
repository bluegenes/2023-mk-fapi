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
    data: bytes # bytes

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

def serialize_sketch(ss) -> bytes:
    """Serialize a sourmash sketch into bytes"""
    buf = BytesIO()
    sourmash.save_signatures([ss], buf, compression=True)
    print(f"serialized sourmash signature into {len(buf.getvalue())} bytes.")
    return buf.getvalue()

@app.post("/query/")
async def create_query_from_file(file: UploadFile):
    """Testing Version: Upload a file of sequences and query mastiff"""
    ### this part should be able to be replaced with JS frontend
    # create a temporary file and save the uploaded file's contents to it
    with NamedTemporaryFile(delete=False) as temp:
        contents = await file.read()
        temp.write(contents)
    sig = sketch_file_to_sourmash(temp.name)
    # delete the temporary file
    os.unlink(temp.name)
    buf = serialize_sketch(sig)
    ### end of JS frontend replacement
    
    http = urllib3.PoolManager()
    try:
        #buf_value = serialize_sketch(query.sig)
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