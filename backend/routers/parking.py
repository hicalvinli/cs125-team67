import os
import time
import json
import httpx
import osmnx as ox
import networkx as nx
from fastapi import APIRouter
from dotenv import load_dotenv
from fastapi import HTTPException
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderServiceError

load_dotenv()
DELTA = 5
WALKING_SPEED = 1.3
METER_URL = "https://data.lacity.org/api/v3/views/s49e-q6j2/query.json"
OCCUPANCY_URL = "https://data.lacity.org/api/v3/views/e7h6-4a3e/query.json"

geolocator = Nominatim(user_agent="cs125_parkwise")

router = APIRouter(
    prefix="/api/parking"
)

def get_lat_lon(address: str) -> (float, float):
    location = geolocator.geocode(address)
    if location:
        # Extract latitude and longitude
        latitude = location.latitude
        longitude = location.longitude
        return latitude, longitude
    else:
        raise ValueError

@router.get("/")
async def get_parking(address: str, date: str, max_walk: int):

    # Geocode provided address
    try:
        lat, lon = get_lat_lon(address)
    except ValueError:
        raise HTTPException(status_code = 400, detail = "Invalid address")
    except GeocoderServiceError as e:
        print(f"Geocoding service error: {e}")
        raise HTTPException(status_code = 503, detail = "Geocoding service unavailable")

    # Calculate radius with walk speed
    radius = max_walk * 60 * WALKING_SPEED

    # Get nearby parking meters
    meter_info = {}
    json_body = {
        "query": f"SELECT * WHERE within_circle(LatLng, {lat}, {lon}, {radius})"
    }
    headers = {
        "X-App-Token": os.getenv("APP_TOKEN"),
        "Content-Type": "application/json"
    }
    async with httpx.AsyncClient() as client:
        response = await client.post(METER_URL, json=json_body, headers=headers)
        response.raise_for_status()
        meter_info = {i["spaceid"]: i for i in response.json()}

        print(f"Found {len(response.json())} total spots near {lat}, {lon}")

        # Get available spots in chunks
        chunk_size = 500
        occupancy_data = []

        space_ids = list(meter_info.keys())
        for i in range(0, len(space_ids), chunk_size):
            chunk = space_ids[i:i + chunk_size]
            id_list = ",".join(f"'{sid}'" for sid in chunk)
            json_body = {
                "query": f"select spaceid, eventtime, occupancystate where spaceid in ({id_list})"
            }
            response = await client.post(OCCUPANCY_URL, json=json_body, headers=headers)
            occupancy_data.extend(response.json())
        occupancy_data = {i['spaceid']: i for i in occupancy_data}

        print(f"Found {len(occupancy_data)} occupancy results")

        # This downloading of graph is a bottleneck, not sure how to fix
        G = ox.graph_from_point((lat, lon), dist = radius + 100, network_type = 'walk')

        start_node = ox.nearest_nodes(G, lon, lat)
        lengths = nx.single_source_dijkstra_path_length(G, start_node, weight = 'length')

        new_meter_info = {}
        for spaceid, meter in meter_info.items():
            spot = occupancy_data.get(spaceid, {"occupancystate": "UNKNOWN", "eventtime": "1970-01-01T00:00:00Z"})
            if spot["occupancystate"] == "UNKNOWN" or spot["occupancystate"] == "VACANT":
                meter["occupancy"] = spot["occupancystate"]
                meter["last_updated"] = spot["eventtime"]

                # Calculate walking distance while making a pass anyway
                dest_node = ox.nearest_nodes(G, meter["latlng"]["longitude"], meter["latlng"]["latitude"])
                dist = float(lengths.get(dest_node, float('inf')))
                minutes = float(dist / WALKING_SPEED / 60)

                # Discard if too much greater than specified walk time
                if minutes > max_walk + DELTA:
                    continue
                else:
                    meter["walk_distance"], meter["walk_time"] = dist, minutes
                    new_meter_info[spaceid] = meter
        meter_info = new_meter_info
        print(f"Found {len(meter_info)} available spots near {lat}, {lon}")

        # Return sorted results in json format
        return json.dumps(dict(sorted(
            meter_info.items(),
            key=lambda item: (item[1]['walk_time'], item[1]['walk_distance'])
        )))