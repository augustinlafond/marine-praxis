import os
from dotenv import load_dotenv
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
import gfwapiclient as gfw
from src.data.grid_creation import create_the_grid
from src.data.download import download_fishing_effort_gfw, download_sar_detection_gfw, download_bathymetry
from src.data.preprocess import preproc_gfw, preproc_bathymetry
import asyncio


async def main():
    # Load environment variables
    load_dotenv()
    gfw_access_token = os.environ.get("GFW_API_ACCESS_TOKEN")

    # Loading the GeoJSON file for the French Mediterranean (ie., the study area)
    study_area = gpd.read_file("https://geo.vliz.be/geoserver/wfs?request=getfeature&service=wfs&version=1.1.0&typename=MarineRegions:eez_iho&outputformat=json&filter=%3COr%3E%3COr%3E%3CPropertyIsEqualTo%3E%3CPropertyName%3Emrgid%3C%2FPropertyName%3E%3CLiteral%3E25185%3C%2FLiteral%3E%3C%2FPropertyIsEqualTo%3E%3CPropertyIsEqualTo%3E%3CPropertyName%3Emrgid%3C%2FPropertyName%3E%3CLiteral%3E25609%3C%2FLiteral%3E%3C%2FPropertyIsEqualTo%3E%3C%2FOr%3E%3CPropertyIsEqualTo%3E%3CPropertyName%3Emrgid%3C%2FPropertyName%3E%3CLiteral%3E25612%3C%2FLiteral%3E%3C%2FPropertyIsEqualTo%3E%3C%2FOr%3E").dissolve()

    # Connect to the GFW API
    gfw_client = gfw.Client(
        access_token=gfw_access_token
        )

    ################## Creation of the spatial grid that defines the study area and the granularity of the features ##################
    ################## We create a 5 km x 5 km spatial grid over the French Mediterranean Sea #######################################

    print("Creating the spatial grid for the study area")

    grid = create_the_grid(study_area)

    # Connection to the PostGIS database
    engine = create_engine("postgresql://augustin:motdepasse@localhost:5432/marine_praxis")

    # Send the spatial table to PostGIS database
    grid.to_postgis("grid_cells", engine, if_exists="replace", index=False)

    print("Spatial grid created and saved")

    ################## Get fishing effort from Global Fishing Watch ##################
    #################################################################################

    # Download data
    print("Downloading fishing effort data from GFW")
    fishing_effort = await download_fishing_effort_gfw(study_area, gfw_client)

    # preproc the data (cleaning, aggregation at the granularity of the spatial grid, etc.)
    print("Preprocessing fishing effort data from GFW")
    fishing_effort = await preproc_gfw(df= fishing_effort,
                                data_type= 'FISHING_EFFORT',
                                grid = grid,
                                vessel_infos = True)

    # Send table to PostGIS database
    print("Sending fishing effort table to PostGIS database")
    fishing_effort.to_postgis("fishing_detailed_features", engine, if_exists="replace", index=False)


    ################## Get SAR vessel detections from Global Fishing Watch ##################
    #########################################################################################

    # Download data
    print("Downloading SAR vessel detections data from GFW")
    sar_detection = await download_sar_detection_gfw(study_area, gfw_client)

    # preproc the data (cleaning, aggregation at the granularity of the spatial grid, etc.)
    print("Preprocessing SAR vessel detection data from GFW")
    sar_detection = await preproc_gfw(df= sar_detection,
                                data_type= 'SAR_DETECTION',
                                grid = grid,
                                vessel_infos = False)

    # Send table to PostGIS database
    print("Sending sar vessel detection table to PostGIS database")
    sar_detection.to_postgis("sar_vessel_detection_features", engine, if_exists="replace", index=False)


    ################## Get bathymetric data from EMODnet ####################################
    #########################################################################################

    # Download data
    print("Downloading bathymetric data")
    download_bathymetry(study_area)

    # Preprocessing the bathymetric data (merge tiles, calculate stats based on grid granulometry)
    print("Preprocessing bathymetric data")
    bathymetry = preproc_bathymetry(grid)

    # Send table to PostGIS database
    print("Sending bathymetric table to PostGIS database")
    bathymetry.to_postgis("bathymetry_features", engine, if_exists="replace", index=False)






if __name__ == "__main__":
    asyncio.run(main())
