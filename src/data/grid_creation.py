import geopandas as gpd
import numpy as np
from shapely.geometry import box
from pathlib import Path

def create_the_grid(study_area):
    """
    Creation of the spatial grid that will serve as a reference for aggregating the project's spatial data
    (fishing effort, bathymetry, water productivity, etc.)
    'study_area' is a geopandas dataframe defining the study area
    """

    # 1. Project to a metric CRS
    # Example: Lambert-93 for mainland France
    study_area = study_area.to_crs(epsg=2154)

    # 2. Set the cell size: 5 km
    cell_size = 5000  # in meters

    # 3. Retrieve the bounds
    xmin, ymin, xmax, ymax = study_area.total_bounds

    # 4. Create the grid coordinates
    x_coords = np.arange(xmin, xmax, cell_size)
    y_coords = np.arange(ymin, ymax, cell_size)

    # 5. Construct the cells
    grid_cells = []
    for x in x_coords:
        for y in y_coords:
            grid_cells.append(box(x, y, x + cell_size, y + cell_size))

    grid = gpd.GeoDataFrame({"geometry": grid_cells}, crs=study_area.crs)

    # 6. Crop the grid to the study area
    grid = gpd.overlay(grid, study_area, how="intersection")

    # 7. Calculate the area of each cell
    grid["area_km2"] = grid.geometry.area / 1e6

    # 8. We remove small polygons (less than 25% of the maximum area of 25 km²)
    grid = grid[grid["area_km2"] >= 0.25 * 25]

    # 9. Add a cell ID
    grid = grid.reset_index(drop=True)
    grid["cell_id"] = np.arange(1, len(grid) + 1)

    # 10. We keep only the columns we want
    grid = grid[["cell_id", "area_km2", "geometry"]]

    # 11. Save
    out = Path(f"../data/processed/grid_5km.gpkg")
    out.parent.mkdir(parents=True, exist_ok=True)

    grid.to_file(out, layer="grid_5km", driver="GPKG")

    return grid
