import geopandas as gpd
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_bounds

from constants.constants import TIF_RESOLUTION

def gdf_to_geotiff(gdf: gpd.GeoDataFrame, output_file: str, resolution: float = TIF_RESOLUTION, dtype: rasterio.dtypes = rasterio.uint8) -> None:
    """
    Convert a GeoDataFrame to a GeoTIFF file
    :param gdf:
    :param output_file:
    :param resolution:
    :return:
    """

    crs = gdf.crs
    x_min, y_min, x_max, y_max = gdf.total_bounds
    heigth = int(((y_max - y_min) / resolution) / 2)
    width = int(((x_max - x_min) / resolution) / 2)

    dataset = rasterio.open(
        output_file,
        'w',
        driver='GTiff',
        height=heigth,
        width=width,
        count=1,
        dtype=dtype,
        crs=crs,
        transform=from_bounds(x_min, y_min, x_max, y_max, width, heigth),
        nodata=0,
        compress='lzw',
        tiled=True
    )

    # Rasterize the GeoSeries to a NumPy array
    rasterized = rasterize(
        [(geometry, 1) for geometry in gdf.geometry],
        out_shape=dataset.shape,
        transform=dataset.transform
    )

    # Write the rasterized data to the new dataset
    dataset.write(rasterized, 1)

    # Close the new dataset
    dataset.close()

    return


