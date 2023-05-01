import argparse
import geopandas as gpd
import pandas as pd
import json


def csv2geojson(csv_file, shp_file, output_file, decimals=2):
    """
    Convert csv to geojson

    :param csv_file:
    :param shp_file:
    :param output_file:
    :param decimals:
    :return: None

    example: csv2geojson.py --csv for_moz_ts_rd20230210T0000Z_population_impacts.csv --shp moz_adm_shapefile.zip --out for_moz_ts_rd20230210T0000Z_population_impacts.geojson
    """

    # Load the shapefile
    shapefile = gpd.read_file(shp_file)

    # Load the CSV file into a Pandas DataFrame, skipping the second row
    df = pd.read_csv(csv_file, skiprows=[1])

    # Keep only the desired columns
    df = df[['admin_code', 'band_1', 'band_5', 'band_11']]

    # Convert columns to floats
    df['band_1'] = df['band_1'].astype(float)
    df['band_5'] = df['band_5'].astype(float)
    df['band_11'] = df['band_11'].astype(float)

    # Group by admin_code and take the mean of each group
    df_grouped = df.groupby('admin_code').mean().astype(int)  # astype(int) is added to convert the values to int for entire individuals

    # Merge the two dataframes on admin_code and ADM2_CODE
    merged = pd.merge(df_grouped, shapefile, left_on='admin_code', right_on='ADM2_CODE')

    # Convert the columns from float to int in the merged DataFrame
    merged['ADM2_CODE'] = merged['ADM2_CODE'].astype(int)
    merged['ADM1_CODE'] = merged['ADM1_CODE'].astype(int)
    merged['ADM0_CODE'] = merged['ADM0_CODE'].astype(int)

    # Write the dataframe into a processed csv
    merged.drop(columns='geometry').to_csv(output_file.replace('.geojson', '_processed.csv'))
    merged_adm2 = merged.drop(columns=['geometry','ADM0_CODE','ADM1_CODE','ADM2_CODE']).groupby(by=['ADM0_NAME','ADM1_NAME','ADM2_NAME'],as_index=False).sum(numeric_only=True)
    merged_adm1 = merged.drop(columns=['geometry','ADM0_CODE','ADM1_CODE','ADM2_CODE']).groupby(by=['ADM0_NAME','ADM1_NAME'],as_index=False).sum(numeric_only=True)
    merged_adm0 = merged.drop(columns=['geometry','ADM0_CODE','ADM1_CODE','ADM2_CODE']).groupby(by=['ADM0_NAME'],as_index=False).sum(numeric_only=True)
    merged_adm2.to_csv(output_file.replace('.geojson', '_adm2_processed.csv'))
    merged_adm1.to_csv(output_file.replace('.geojson', '_adm1_processed.csv'))
    merged_adm0.to_csv(output_file.replace('.geojson', '_adm0_processed.csv'))

    # Write the GeoDataFrame to a GeoJSON file
    gdf = gpd.GeoDataFrame(merged)
    gdf.to_file(output_file, driver='GeoJSON')

    # compacting geojson even further, by removing blank spaces
    with open(output_file, 'r') as f:
        data = json.load(f)

    with open(output_file, 'w') as f:
        f.write(json.dumps(data, separators=(',', ':')))

    return merged_adm0, merged_adm1, merged_adm2


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert a CSV file to a GeoJSON file.')
    parser.add_argument('--csv', dest='csv_file', type=str, required=True, help='the path to the input CSV file')
    parser.add_argument('--shp', dest='shp_file', type=str, required=True, help='the path to the shapefile')
    parser.add_argument('--out', dest='output_file', type=str, required=True, help='the path to the output GeoJSON file')
    parser.add_argument('--decimals', dest='decimals', type=int, default=2, help='number of decimals in the output GeoJSON file (default: 2)')
    args = parser.parse_args()

    merged_adm0, merged_adm1, merged_adm2 = csv2geojson(args.csv_file, args.shp_file, args.output_file, args.decimals)

