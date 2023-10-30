#Country and regions list
COUNTRY_LIST = ['Ivory Coast', 'Madagascar', 'Malawi', 'Mozambique'] # ['Ghana', 'Ivory Coast', 'Madagascar', 'Malawi', 'Mozambique', 'Togo']

COUNTRY_DICT = {
    'Ghana': 'gha',
    'Ivory Coast': 'civ',
    'Madagascar': 'mdg',
    'Malawi': 'mwi',
    'Mozambique': 'moz',
    'Togo': 'tgo',
}

REGION_DICT = {
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

AEP_THRESHOLDS = {
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

YLT_THRESHOLDS = {
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

SELECTED_RP = [2, 5, 20, 100]

# Maximum number of rows for economic loss to be written in the JSON file
N_ROWS = 100