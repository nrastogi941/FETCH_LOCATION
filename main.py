# fetch_location.py

from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
import pandas as pd
import requests
import os
import uuid
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

LOCATIONIQ_API_KEY = os.getenv("LOCATIONIQ_API_KEY")
if not LOCATIONIQ_API_KEY:
    raise ValueError("LOCATIONIQ_API_KEY environment variable not set.")

def get_location_from_pincode(pincode: str):
    try:
        response = requests.get(
            f"https://us1.locationiq.com/v1/search.php",
            params={
                "key": LOCATIONIQ_API_KEY,
                "q": pincode,
                "format": "json"
            }
        )
        if response.status_code == 200:
            data = response.json()
            if data:
                return {
                    "latitude": data[0]["lat"],
                    "longitude": data[0]["lon"],
                    "display_name": data[0]["display_name"]
                }
    except Exception as e:
        print(f"Error for pincode {pincode}: {e}")
    return {
        "latitude": None,
        "longitude": None,
        "display_name": None
    }

@app.post("/upload/")
async def upload_csv(file: UploadFile = File(...)):
    # Read CSV into DataFrame
    df = pd.read_csv(file.file)
    
    # Assume column name is 'pincode'
    if 'pincode' not in df.columns:
        return {"error": "CSV must contain a 'pincode' column."}

    # Add columns for location data
    df["latitude"] = None
    df["longitude"] = None
    df["location"] = None

    for idx, row in df.iterrows():
        pincode = str(row["pincode"])
        location_data = get_location_from_pincode(pincode)
        df.at[idx, "latitude"] = location_data["latitude"]
        df.at[idx, "longitude"] = location_data["longitude"]
        df.at[idx, "location"] = location_data["display_name"]

    # Save updated CSV
    output_filename = f"output_{uuid.uuid4().hex[:8]}.csv"
    df.to_csv(output_filename, index=False)

    return FileResponse(output_filename, media_type="text/csv", filename=output_filename)
