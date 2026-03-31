import gfwapiclient as gfw
import numpy as np
import requests
from pathlib import Path

# Root of the project directory
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Directory to save the bathymetric tiles
BATHY_DIR = PROJECT_ROOT / "data" / "raw" / "emodnet_bathy_mediterranee"

async def download_fishing_effort_gfw(study_area, gfw_client):
    """
    Download fishing effort data from Global Fishing Watch
    """

    # Download the data
    result = await gfw_client.fourwings.create_fishing_effort_report(
        spatial_resolution="HIGH",
        temporal_resolution="ENTIRE",
        group_by="VESSEL_ID",
        start_date="2025-01-01",
        end_date="2026-01-01",
        geojson=study_area.to_geo_dict()["features"][0]["geometry"]
    )

    return result.df()


async def download_sar_detection_gfw(study_area, gfw_client):
    """
    Download Synthetic Aperture Radar (SAR) vessel detection data from Global Fishing Watch
    """

    # Request SAR data in 2025 in the study area (at the resolution of the vessel)
    result = await gfw_client.fourwings.create_sar_presence_report(
        spatial_resolution="HIGH",
        temporal_resolution="ENTIRE",
        group_by="VESSEL_ID",
        start_date="2025-01-01",
        end_date="2026-01-01",
        geojson=study_area.to_geo_dict()["features"][0]["geometry"]
    )

    return result.df()


def download_bathymetry(study_area):
    # It is not possible to download the entire set of bathymetric data for the study area directly. For this reason, we are querying smaller bounding boxes.
    # We retrieve the coordinates of the bounding box for the study area
    xmin, ymin, xmax, ymax = study_area.bounds.values[0]

    # We create the boundaries of the new, smaller bounding boxes. Here, 9 bounding boxes are created that cover the entire study area.
    x_bbox = np.linspace(xmin, xmax, 4)
    y_bbox = np.linspace(ymin, ymax, 4)

    bboxes = [[x_bbox[i], y_bbox[j], x_bbox[i+1], y_bbox[j+1]] for i in range(len(x_bbox)-1) for j in range(len(y_bbox)-1)]

    # We download the data for each bbox and save it in a tiff format
    base_url = "https://ows.emodnet-bathymetry.eu/wcs"

    for i in range(len(bboxes)):
        params = {
            "SERVICE": "WCS",
            "VERSION": "1.0.0",
            "REQUEST": "GetCoverage",
            "COVERAGE": "emodnet:mean_2022",
            "CRS": "EPSG:4326",
            "BBOX": ",".join([str(coord) for coord in bboxes[i]]),
            "FORMAT": "GeoTIFF",
            "resx": "0.00208333",
            "resy": "0.00208333",
        }

        r = requests.get(base_url, params=params, timeout=120)

        print("status:", r.status_code)
        print("content-type:", r.headers.get("Content-Type"))

        if "tiff" in str(r.headers.get("Content-Type", "")).lower():
            BATHY_DIR.mkdir(parents=True, exist_ok=True)
            out = BATHY_DIR / f"tile_{i}.tif"
            out.write_bytes(r.content)
            print("GeoTIFF téléchargé :", out)
        else:
            print(r.text[:1000])
