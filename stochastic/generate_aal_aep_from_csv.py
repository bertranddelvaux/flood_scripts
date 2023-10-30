import os
import json

import pandas as pd
import numpy as np

from constants import *


def generate_aal_aep_people_impacted(country):

    json_dict = {}

    for i, thresh in enumerate(AEP_THRESHOLDS[country]):

        json_dict[f'thresh_{i}'] = {}

        # Read the AEP (different return periods) for the country
        rp_csv_country = os.path.join(COUNTRY_DICT[country], f'{thresh}_gul_S1_leccalc_wheatsheaf_aep.csv')
        df_country = pd.read_csv(rp_csv_country).drop(columns=['countrycode', 'sidx'])

        # Create the nested JSON structure
        result = {}
        for _, row in df_country.iterrows():
            if row['return_period'] not in result:
                result[int(row['return_period'])] = {}
            result[row['return_period']] = row['loss']

        json_dict[f'thresh_{i}']['adm0'] = result

        # json_file = os.path.join(country_dict[country], f'population/aep_adm0_thresh_{i}.json')
        #
        # # Write the JSON data to the file
        # with open(json_file, 'w') as f:
        #     json.dump(result, f, indent=4)

        # Read the AEP (different return periods)
        rp_csv = os.path.join(COUNTRY_DICT[country], f'{thresh}_gul_S2_leccalc_wheatsheaf_aep.csv')
        df = pd.read_csv(rp_csv).drop(columns=['sidx'])

        # Create the nested JSON structure
        result = {}
        for _, row in df.iterrows():
            if row['geogname1'] not in result:
                result[row['geogname1']] = {}
            result[row['geogname1']][str(int(row['return_period']))] = row['loss']

        json_dict[f'thresh_{i}']['adm1'] = result

        # json_file = os.path.join(country_dict[country], f'population/aep_adm1_thresh_{i}.json')
        #
        # # Write the JSON data to the file
        # with open(json_file, 'w') as f:
        #     json.dump(result, f, indent=4)

    json_file = os.path.join('jsons', COUNTRY_DICT[country], f'population.json')

    # Write the JSON data to the file
    with open(json_file, 'w') as f:
        json.dump(json_dict, f, indent=4)


def generate_ylt_economic_loss(country, power_factor=2):

    json_dict = {}

    for i, thresh in enumerate(YLT_THRESHOLDS[country]):

        json_dict[f'thresh_{i}'] = {}

        for adm_level, adm_file in enumerate([1, 2]):

            json_dict[f'thresh_{i}'][f'adm{adm_level}'] = {}

            # Read the YLT for the country
            f_name = os.path.join(COUNTRY_DICT[country], f'YLT/{thresh}_S{adm_file}_pltcalc.csv')
            df_ylt_all = pd.read_csv(f_name)

            # if geogname in columns, group by geogname first and apply the rest of the code
            if 'geogname1' in df_ylt_all.columns:
                regions = df_ylt_all['geogname1'].unique().tolist()
            else:
                regions = ['all']

            for region in regions:

                if region != 'all':
                    df_ylt = df_ylt_all[df_ylt_all['geogname1'] == region]
                else:
                    df_ylt = df_ylt_all

                df_ylt_sorted = df_ylt.sort_values(by=['loss'])
                loss_sorted = df_ylt_sorted['loss'].values

                df_ylt_sorted['p'] = 1.0 * np.arange(len(df_ylt_sorted)) / float(len(df_ylt_sorted) - 1)

                # Create a new DataFrame with 'p' values distributed according to a power function
                p_values = np.linspace(0, 1, N_ROWS)  # Create 'p' values between 0 and 1
                p_values = 1 - (1 - p_values) ** power_factor  # Apply the power function

                # Create a new DataFrame with equally spaced 'loss' values
                compact_df = pd.DataFrame()
                compact_df['p'] = p_values
                compact_df['loss'] = np.interp(compact_df['p'], df_ylt_sorted['p'], df_ylt_sorted['loss'])

                # Create a list of dictionaries
                if region != 'all':
                    json_dict[f'thresh_{i}'][f'adm{adm_level}'][region] = {}
                    json_dict[f'thresh_{i}'][f'adm{adm_level}'][region]['loss'] = compact_df['loss'].values.tolist()
                    json_dict[f'thresh_{i}'][f'adm{adm_level}'][region]['p'] = compact_df['p'].values.tolist()
                else:
                    json_dict[f'thresh_{i}'][f'adm{adm_level}'] = {}
                    json_dict[f'thresh_{i}'][f'adm{adm_level}']['loss'] = compact_df['loss'].values.tolist()
                    json_dict[f'thresh_{i}'][f'adm{adm_level}']['p'] = compact_df['p'].values.tolist()
                    #json_dict[f'thresh_{i}']['adm0'] = [{'loss': loss, 'p': prob} for loss, prob in zip(loss_sorted, p)]

        # Define the output JSON file name
        json_file = os.path.join('jsons', COUNTRY_DICT[country], f'economic.json')

        # Write the list of dictionaries to the file
        with open(json_file, 'w') as f:
            json.dump(json_dict, f, indent=4)




for country in COUNTRY_LIST:
    generate_aal_aep_people_impacted(country)
    generate_ylt_economic_loss(country)

