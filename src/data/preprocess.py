import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon
from dotenv import load_dotenv
import gfwapiclient as gfw
import os
import pandas as pd
import rasterio
from rasterio.merge import merge
from rasterstats import zonal_stats
from src.data.download import BATHY_DIR, PROJECT_ROOT

# Load environment variables
load_dotenv()
gfw_access_token = os.environ.get("GFW_API_ACCESS_TOKEN")


def gridded_data_gfw(df, res):
    """
    Based on lat/lon fishing effort coordinates, this function creates a spatial grid at the spatial resolution of the parameter 'res'.
    """
    lat_min = np.min(df.lat)
    lon_min=np.min(df.lon)
    lat_max = np.max(df.lat)
    lon_max=np.max(df.lon)

    cols = np.arange(lon_min-res, lon_max+res, res).tolist()
    rows = np.arange(lat_min-res, lat_max+res, res).tolist()

    # Create the polygons based on lat/lon coordinates returned by the Global Fishing Watch API which corresponds to the centers of the pixels.
    polygons = []
    for x in cols[:-1]:
        for y in rows[:-1]:
            polygons.append(Polygon([(x,y), (x+res, y), (x+res, y+res), (x, y+res)]))

    gfw_grid = gpd.GeoDataFrame({'geometry':polygons}).set_crs(4326) # Data returned by the API are in EPSG:4326

    gfw_spatial_points = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.lon, df.lat),
        crs="EPSG:4326"
        )

    # Spatial join between the newly created grid and the points of coordinates returned by the API
    result = gfw_grid.sjoin(gfw_spatial_points, how="left")

    result.drop(columns="index_right", inplace=True)
    result.to_crs(2154, inplace=True)
    result["area_km2_gfw_cell"] = result.area/1e6

    return result


def clean_data_gfw(gdf):
    # Remove NaN, reset index
    gdf = gdf[~np.isnan(gdf["lat"])].reset_index(drop=True)

    # Create a GFW cell id
    gfw_cell_ids = gdf[["lat", "lon"]].drop_duplicates().sort_values(by=["lat", "lon"])
    gfw_cell_ids["gfw_cell_id"] = np.arange(1, len(gfw_cell_ids) + 1)
    gdf = gdf.merge(gfw_cell_ids, how="left", on = ["lat", "lon"])

    # One hot encoding the column "vessel_type".
    # We need to to this because we'll then agregate the data to higher spatial level

    vessel_types = pd.get_dummies(gdf['gear_type'], prefix= "vessel_type")
    vessel_types.columns = vessel_types.columns.str.lower()
    vessel_types = vessel_types.astype(int)

    # Adding to the dataframe the one hot encoded columns
    gdf.loc[:,vessel_types.columns] = vessel_types
    gdf.drop(columns="gear_type",inplace = True)

    return gdf, vessel_types.columns



async def get_vessel_characteristics(df):
    """A function that retrieves the tonnage and size of ships based on their identification number"""

    # Connect to the GFW API
    gfw_client = gfw.Client(
        access_token=gfw_access_token,
    )

    # Get all vessel ids. They will be used to call the GFW vessel API
    vessel_ids = list(df["vessel_id"].unique())

    # Remove empty ids
    vessel_ids = [id for id in vessel_ids if id != ""]

    # Because of the limited number of ships that can be queried via the API, we create a loop that retrieves information for 1,000 ships at a time
    for i in range(0, len(vessel_ids), 1000):
        temp = await gfw_client.vessels.get_vessels_by_ids(
            ids = vessel_ids[i:i+1000]
            )
        if i == 0:
            vessels_result = temp.df()
        else:
            vessels_result = pd.concat([vessels_result, temp.df()]) # The results are concatenated

    # Creating a list of dictionaries that will contains the characteristics of the vessels
    records = []

    for _, row in vessels_result.iterrows():

        vessel_id = [vessel_id["id"] for vessel_id in row["self_reported_info"]] # Care is taken to retrieve all identifiers.

        if len(row["registry_info"]) > 0:
            length_m = row["registry_info"][0]["length_m"]
            tonnage_gt = row["registry_info"][0]["tonnage_gt"]

        else:
            length_m, tonnage_gt = [np.nan] * 2

        gear_types = row["combined_sources_info"][0]["gear_types"][0]["name"]

        records.append({"vessel_id" : vessel_id,
                    "length_m" : length_m,
                    "tonnage_gt" : tonnage_gt,
                    "gear_types": gear_types
                    })

    vessel_infos_df = pd.DataFrame({"vessel_id": vessel_ids}).merge(pd.DataFrame(records).explode("vessel_id"), how="left", on="vessel_id")

    return vessel_infos_df.groupby("vessel_id", as_index = False).agg({"length_m": "mean", # In rare cases, a given vessel_id may be associated with different tonnage and length values; in such cases, duplicate rows are removed and replaced with the average
                                                                        "tonnage_gt": "mean",
                                                                        "gear_types": "first"})


def data_aggregation_gfw(gdf, data_type, vessel_types, grid):
    """
    The function aggregate the GFW data at the granularity of the spatial grid cell we defined for the study area.
    'vessel_types' is the list of column names associated with vessel types following one-hot encoding (see clean_gfw_data function)
    'data_type' takes as argument 'FISHING_EFFORT' or 'SAR_DETECTION'"
    """

    # Groupby data over gfw cells
    # We first need to create a dictionnary to pass in the agg method
    agg_dic = {col: "sum" for col in vessel_types}
    agg_dic.update({
        "area_km2_gfw_cell": "first",
        "geometry": "first"
    })

    if data_type == 'FISHING_EFFORT':
        agg_dic.update({
            "length_m": "mean",
            "tonnage_gt": "mean",
            "hours": "sum"
        })
        # we filter the columns we want to keep
        gdf = gdf.filter(regex="geometry|gfw_cell_id|area_km2_gfw_cell|length_m|tonnage_gt|hours|vessel_type_", axis = 1)

    elif data_type == 'SAR_DETECTION':
        agg_dic.update({
            "detections": "sum"
        })

        gdf = gdf.filter(regex="geometry|gfw_cell_id|area_km2_gfw_cell|detections|vessel_type_", axis = 1)


    gdf = gdf.groupby("gfw_cell_id", as_index = False).agg(agg_dic)
    gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs= 2154)

    # Overlay with the spatial grid to crop overlapping gfw cells
    gdf = gdf.overlay(grid, how='intersection')

    # Weighting of columns based on the ratio of GFW cell area before and after overlay
    gdf["intersect_grid_ratio"] = round((gdf.area/1e6)/gdf["area_km2_gfw_cell"],4)

    df_to_weigth = gdf.filter(regex="vessel_type_|hours|detections", axis = 1)
    gdf[df_to_weigth.columns] = df_to_weigth.mul(gdf["intersect_grid_ratio"], axis = 0)

    # Groupby data over grid cells
    result = grid.drop(columns=["area_km2"]).sjoin(gdf.drop(columns=["gfw_cell_id", "area_km2_gfw_cell", "cell_id", "area_km2", "intersect_grid_ratio"]), how="left", predicate="contains")
    del agg_dic['area_km2_gfw_cell']
    result = result.drop(columns=["index_right"]).groupby("cell_id", as_index = False).agg(agg_dic)
    result = gpd.GeoDataFrame(result.replace(to_replace = 0, value = np.nan), geometry="geometry", crs = 2154)

    return result

async def preproc_gfw(df, data_type, grid, vessel_infos = False):
    """
    Complete preproc pipeline for GFW data.
    """

    # Convert lat/lon data to a spatial grid at the resolution of the GFW data
    result = gridded_data_gfw(df, res = 0.01)

    # Clean data and one hot encoding of the column 'vessel_type'
    result, vessel_types = clean_data_gfw(result)

    # Adding vessel length and tonnage
    if vessel_infos:
        vessel_infos = await get_vessel_characteristics(result)

        result = result.merge(vessel_infos, how="left", on="vessel_id")

    result = data_aggregation_gfw(result, data_type, vessel_types, grid)

    return result


def tiles_merging_bathymetry():
    """
    A function to merge all tiles to get the bathymetric data over the entire study zone
    """

    tif_files = [str(BATHY_DIR) + "/" + tile for tile in os.listdir(BATHY_DIR)]

    print("Files found:", tif_files)

    print("Opening GeoTIFF files...")

    # Open all the GeoTIFF files
    src_files_to_mosaic = [rasterio.open(str(fp)) for fp in tif_files]

    print("Merging rasters...")

    # Merge the rasters
    mosaic, out_trans = merge(src_files_to_mosaic)

    # Copy the metadata from the first file and update it for the merged raster
    out_meta = src_files_to_mosaic[0].meta.copy()
    out_meta.update({
        "driver": "GTiff",
        "height": mosaic.shape[1],
        "width": mosaic.shape[2],
        "transform": out_trans
    })

    # Save the merged raster to disk
    with rasterio.open(PROJECT_ROOT / "data" / "processed" / "bathymetry_merged.tif", "w", **out_meta) as dest:
        dest.write(mosaic)

    # Close all open files
    for src in src_files_to_mosaic:
        src.close()

    print("Bathymetry merged! The merged file is saved as 'bathymetry_merged.tif' in data/processed.")


def aggregation_bathymetry(grid):
    """
    A function to calculate statistics (mean/min/max depth) at the granularity of the spatial grid.
    """

    stats = zonal_stats(
    grid.to_crs(4326),
    str(PROJECT_ROOT) + "/data/processed/bathymetry_merged.tif",
    stats=["min", "max", "mean", "std"],
    geojson_out=False)

    return pd.concat([grid, pd.DataFrame(stats)], axis = 1).drop(columns="area_km2")

def preproc_bathymetry(grid):
    tiles_merging_bathymetry()
    return aggregation_bathymetry(grid)
