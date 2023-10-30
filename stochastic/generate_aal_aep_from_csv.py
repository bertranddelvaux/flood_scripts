import os
import json

import pandas as pd
import numpy as np

#Country and regions list
country_list = ['Ivory Coast', 'Madagascar', 'Malawi', 'Mozambique'] # ['Ghana', 'Ivory Coast', 'Madagascar', 'Malawi', 'Mozambique', 'Togo']

country_dict = {
    'Ghana': 'gha',
    'Ivory Coast': 'civ',
    'Madagascar': 'mdg',
    'Malawi': 'mwi',
    'Mozambique': 'moz',
    'Togo': 'tgo',
}

region_dict = {
    'Mozambique':
        ['Niassa', 'Cabo Delgado', 'Nampula', 'Maputo', 'Tete', 'Zambezia','Manica','Sofala','Gaza',
         'Inhambane','Cidade de Maputo',
        ],
    'Malawi':
        ['Northern Region', 'Area under National Administration','Central Region','Southern Region',
        ],
    'Madagascar':
        ['Diana', 'Sava','Sofia','Boeny','Analanjirofo','Melaky','Alaotra Mangoro','Betsiboka', 'Menabe','Bongolava', 'Analamanga',
        'Atsinanana', 'Itasy','Vakinankaratra', "Amoron'i Mania",'Vatovavy Fitovinany','Atsimo Andrefana',
        'Haute Matsiatra','Ihorombe', 'Anosy','Atsimo Atsinanana','Androy',
        ]
}

aep_thresholds = {
    'Ghana':
        ['ANA_20_GH_POP', 'ANA_21_GH_POP', 'ANA_22_GH_POP'],
    'Ivory Coast':
        ['JG_20220518_2021s1110_ANA03_CI_01', 'JG_20220518_2021s1110_ANA04_CI_01', 'JG_20220518_2021s1110_ANA05_CI_01'],
    'Madagascar':
        ['JG_20220520_2021s1110_ANA03_MD_01', 'JG_20220520_2021s1110_ANA04_MD_01', 'JG_20220607_2021s1110_ANA05_MG_01'],
    'Malawi':
        ['JG_20220608_2021s1110_ANA03_MW_01', 'JG_20220608_2021s1110_ANA04_MW_01', 'JG_20220609_2021s1110_ANA05_MW_01'],
    'Mozambique':
        ['JG_20220427_2021s1110_ANA07_MZ_02', 'JG_20220427_2021s1110_ANA08_MZ_02', 'JG_20220427_2021s1110_ANA09_MZ_01'],
    'Togo':
        ['ANA_21_TG_POP', 'ANA_22_TG_POP', 'ANA_23_TG_POP'],
}

ylt_thresholds = {
    'Ghana':
        ['ANA_17_GH_GDP_gul'],
    'Ivory Coast':
        ['ANA_06_CI_GDP_gul'],
    'Mozambique':
        ['ANA_05_MZ_GDP_gul', 'ANA_06_MZ_GDP_gul'],
    'Malawi':
        ['ANA_01_MW_GDP_gul', 'ANA_02_MW_GDP_gul'],
    'Madagascar':
        ['ANA_01_MD_GDP_gul', 'ANA_02_MG_GDP_gul'],
    'Togo':
        ['ANA_18_TG_GDP_gul'],
}

selected_rp = [2, 5, 20, 100]


def generate_aal_aep_people_impacted(country):

    json_dict = {}

    for i, thresh in enumerate(aep_thresholds[country]):

        json_dict[f'thresh_{i}'] = {}

        # Read the AEP (different return periods) for the country
        rp_csv_country = os.path.join(country_dict[country], f'{thresh}_gul_S1_leccalc_wheatsheaf_aep.csv')
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
        rp_csv = os.path.join(country_dict[country], f'{thresh}_gul_S2_leccalc_wheatsheaf_aep.csv')
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

    json_file = os.path.join('jsons', country_dict[country], f'population.json')

    # Write the JSON data to the file
    with open(json_file, 'w') as f:
        json.dump(json_dict, f, indent=4)


def generate_ylt_economic_loss(country):

    json_dict = {}

    for i, thresh in enumerate(ylt_thresholds[country]):

        json_dict[f'thresh_{i}'] = {}

        # Read the YLT for the country
        f_name = os.path.join(country_dict[country], f'YLT/{thresh}_S1_pltcalc.csv')
        df_ylt = pd.read_csv(f_name)

        loss_sorted = np.unique(np.sort(df_ylt['loss'].values))

        p = 1.0 * np.arange(len(loss_sorted)) / float(len(loss_sorted) - 1)

        # Create a list of dictionaries
        json_dict[f'thresh_{i}']['adm0'] = {}
        json_dict[f'thresh_{i}']['adm0']['loss'] = loss_sorted.tolist()
        json_dict[f'thresh_{i}']['adm0']['p'] = p.tolist()
        #json_dict[f'thresh_{i}']['adm0'] = [{'loss': loss, 'p': prob} for loss, prob in zip(loss_sorted, p)]

        # Define the output JSON file name
        json_file = os.path.join('jsons', country_dict[country], f'economic.json')

        # Write the list of dictionaries to the file
        with open(json_file, 'w') as f:
            json.dump(json_dict, f, indent=4)




for country in country_list:
    generate_aal_aep_people_impacted(country)
    generate_ylt_economic_loss(country)

