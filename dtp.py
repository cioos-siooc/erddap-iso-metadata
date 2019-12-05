import os, sys, getopt
import subprocess
from datetime import datetime
import pytz
import importlib
import re
import yaml
import pandas as pd
import copy
from erddapy import ERDDAP

from xml.sax.saxutils import escape # use defusedxml instead
from yamlinclude import YamlIncludeConstructor
from googletrans import Translator
# from translate import Translator


import configparser
import logging
import argparse

logging.basicConfig(filename='dtp.log', level=logging.DEBUG)

dtp_logger = logging.getLogger(__name__)

# runs all the things
def main(prog_args):
    YamlIncludeConstructor.add_to_loader_class(loader_class=yaml.FullLoader)

    print('read_config...')
    dtp_config = read_config(prog_args)
    print(dtp_config)

    print('load_driver...')
    dtp_driver = load_driver(dtp_config)
    print(dtp_driver)

    print('load_data_source...')
    data_source = load_data_source(dtp_config, dtp_driver)
    #print(list(data_source))

    print('transform_data_source...')
    pygm_source = transform_data_source(dtp_config, data_source)
    #print(pygm_source)

    print('translate_into_yaml...')
    pygm_yaml = translate_into_yaml(dtp_config, pygm_source)
    #print(pygm_yaml)

    print('output_yaml_source...')
    final_result = output_yaml_source(dtp_config, pygm_yaml)
    #print(pygm_yaml)

    if dtp_config['output']['pre_processor']:
        print("Running pre_processor: %s" % (dtp_config['output']['pre_processor']))
        dtp_logger.info("Running pre_processor: %s" % (dtp_config['output']['pre_processor']))

        out = subprocess.run(dtp_config['output']['pre_processor'])

        print(out)
        dtp_logger.debug(out)


    translate_to_xml(dtp_config, pygm_yaml)

    if dtp_config['output']['post_processor']:
        print("Running post_processor: %s" % (dtp_config['output']['post_processor']))
        dtp_logger.info("Running post_processor: %s" % (dtp_config['output']['post_processor']))

        out = subprocess.run(dtp_config['output']['post_processor'])

        print(out)
        dtp_logger.debug(out)

    print('Exiting...')
    pass

# loads configuration data
def read_config(prog_args):
    config = configparser.ConfigParser()
    
    if prog_args.config:
        print("Using supplied configuration: %s" % (prog_args.config))
        config.read(prog_args.config)
    else:
        print("No configuration file specified, using default: dtp_config.ini")
        config.read('dtp_config.ini')

    return config

# load data source 'driver'
def load_driver(config):
    # not currently being used, will draw on later to merge in SharePoint data
    driver = importlib.import_module('drivers.' + config['driver']['driver_type'])
    return driver

# loads data from data source via 'driver'
def load_data_source(config, driver):
    data = {}
    print('Loading baseline station data from ERDDAP...')
    data['erddap'] = load_data_from_erddap(config)

    for index_label, station_profile in enumerate(data['erddap']):
        print('Loading Individual Profile: %s' % (station_profile))
        data['erddap'][station_profile] = load_data_from_erddap(config, station_profile, data['erddap'][station_profile])
        
    
    # Harvest Station data from ERDDAP metadata, fields, units, etc.  Use OpenDAP .DAS feed
    #data['maintenance'] = driver.Driver.load_data(config)
    return data

def load_data_from_erddap(config, station_id=None, station_data=None):
    # TODO: Fix and account for these issues
    # YAML Input Currently Requires:
    # - summary_fra (with content)
    # - title_fra (with content)
    # - keywords processed to strip out characters like ">" and "/"
    # - contributor_name (with content)
    mcf_template = yaml.safe_load(open(config['static_data']['mcf_template'], 'r'))

    translate = config['static_data']['translate'].split('|')
    translator = Translator()
    #translator = Translator(to_lang=translate[1])

    es = ERDDAP(
        server=config['dynamic_data']['erddap_server'],
        protocol=config['dynamic_data']['erddap_protocol'],
    )

    eov_list = config['static_data']['eov_list'].split(',')

    if station_id is None:
        #load all station data MCF skeleton
        stations = {}
        es.dataset_id = 'allDatasets'

        # filter out "log in" datasets as the vast majoirty of their available metadata is unavailable
        es.constraints = {'accessible=': 'public'}
        stations_df = es.to_pandas()

        # drop 'allDatasets' row
        stations_df.drop(labels=0, axis='index', inplace=True)
        # print(stations_df)

        for index_label, row_series in stations_df.iterrows():
            id = row_series['datasetID']
            
            # ensure each station has an independant copy of the MCF skeleton
            stations[id] = copy.deepcopy(mcf_template)
            dataset_url = row_series['tabledap'] if row_series['dataStructure'] == 'table' else row_series['griddap']

            stations[id]['id'] = escape(id)
            stations[id][config['dynamic_data']['dataset_url_field']] = escape(dataset_url)
            
            #stations[id]['spatial']['datatype'] = 'textTable' if row_series['dataStructure'] == 'table' else 'grid'

            stations[id]['geospatial_lon_min'] = row_series['minLongitude (degrees_east)']
            stations[id]['geospatial_lat_min'] = row_series['minLatitude (degrees_north)']
            stations[id]['geospatial_lon_max'] = row_series['maxLongitude (degrees_east)']
            stations[id]['geospatial_lat_max'] = row_series['maxLatitude (degrees_north)']

            for field_name in config['dynamic_data']['global_translation_fields'].split(','):
                print("Processing Translation Field: %s for dataset %s" % (field_name, id))
                stations[id][field_name] = re.sub('[\r\n]', '', row_series[field_name]).replace(': ', ' - ')

                if translate[0] == 'en' and stations[id][field_name + '_fra'] == '':
                    alt_lang = 'fra'
                elif translate[0] == 'fr' and stations[id][field_name + '_eng'] == '':
                    alt_lang = 'eng'

                print("Translating %s to %s" % (translate[0], translate[1]))
                try:
                    alt_text = translator.translate(stations[id][field_name], src=translate[0], dest=translate[1]).text
                    #alt_text = translator.translate(stations[id][field_name])

                    stations[id][field_name + '_' + alt_lang] = escape(alt_text)
                except Exception as jde:
                    print("Field: %s, From: %s to %s \"%s\", ERROR: %s" % (field_name, translate[0], translate[1], stations[id][field_name], jde))

                stations[id][field_name] = escape(stations[id][field_name])

            stations[id]['date_created'] = row_series['minTime (UTC)']
            stations[id]['date_modified'] = row_series['maxTime (UTC)']
            stations[id]['time_coverage_start'] = row_series['minTime (UTC)']
            stations[id]['time_coverage_end'] = row_series['maxTime (UTC)']

        print('Stations after ERDDAP call...')
        # print(stations)

        return_value = stations

    else:
        #load specific station data into MCF skeleton
        print('Loading ERDDAP metadata for station: %s' % (station_id))

        keywords_field = config['dynamic_data']['keywords_field']

        es.dataset_id = station_id

        metadata_url = es.get_download_url(dataset_id='%s/index' % (station_id), response='csv', protocol='info')
        metadata = pd.read_csv(filepath_or_buffer=metadata_url)
        print(metadata_url)
        # print(metadata.head())

        # ERDDAP ISO XML provides a list of dataset field names (long & short), data types & units
        # of measurement, in case this becomes useful for the CIOOS metadata standard we can extend 
        # the YAML skeleton to include these and the template to export them.
        #
        # below most varible attributes from ERDDAP are extracted and pivoted to describe the field
        # actual field data types are extracted seperately and merged into the pivoted dataframe 
        # for completeness
        columns_pivot = metadata[(metadata['Variable Name'] != 'NC_GLOBAL') & (metadata['Row Type']!='variable')].pivot(index='Variable Name', columns='Attribute Name', values='Value')
        col_data_types = metadata[(metadata['Row Type']=='variable')][['Variable Name','Data Type']]
        df_merge = pd.merge(columns_pivot, col_data_types, on='Variable Name')

        station_data['dataset'] = {}
        
        # TODO: UPDATE TO USE variable_1_x_blah notation
        for index_label, field_series in df_merge.iterrows():
            field_name = field_series['Variable Name']
            station_data['dataset'][field_name] = {}
            station_data['dataset'][field_name]['long_name'] = field_series['long_name']
            station_data['dataset'][field_name]['data_type'] = field_series['Data Type']
            station_data['dataset'][field_name]['units'] = field_series['units']

        # station_data[field] = metadata[(metadata['Variable Name']=='NC_GLOBAL') & (metadata['Attribute Name']==field)]['Value'].values[0]

        keywords_prep = metadata[(metadata['Variable Name']=='NC_GLOBAL') & (metadata['Attribute Name']==keywords_field)]['Value'].values[0]

        station_data[keywords_field] = keywords_prep.replace(' > ', ',').replace('/', ',').replace(', ', ',').replace('\n', ' ')

        alt_lang_keywords = []
        
        for eov in eov_list:
            eov = eov.strip()
            if re.search(eov, station_data[keywords_field], re.IGNORECASE):
                alt_lang_keywords.append(eov)

        if translate[0] == 'en' and station_data[keywords_field + '_fra'] == '':
            alt_lang = 'fra'

        elif translate[0] == 'fr' and station_data[keywords_field + '_eng'] == '':
            alt_lang = 'eng'

        station_data[keywords_field + '_' + alt_lang] = escape(','.join(alt_lang_keywords))

        station_data[keywords_field] = escape(station_data[keywords_field])

        for index, field in enumerate(config['static_data']['opt_rec_variables'].split(',')):
            field = field.strip()
            try:
                station_data[field] = escape(metadata[(metadata['Variable Name']=='NC_GLOBAL') & (metadata['Attribute Name']==field)]['Value'].values[0])
            except:
                dtp_logger.info('Field: %s in dataset %s not found in NC_GLOBAL' % (field, station_id))
                

        return_value = station_data


    return return_value

# transforms data from data source into python objects ready for pyYaml -> pyGeometa
def transform_data_source(config, data_source):
    pygm_source = data_source

    return pygm_source

# translate python object to pyYaml output
def translate_into_yaml(dtp_config, pygm_source):
    pygm_yaml = pygm_source

    return pygm_yaml

def output_yaml_source(dtp_config, pygm_yaml):
    output = []

    # TODO: use new config output settings and map to metadata-xml genereator
    for index_label, station_profile in enumerate(pygm_yaml['erddap']):
        dtp_logger.info('Dumping YAML for %s profile' % (station_profile))

        file_name = '%s/%s.yml' % (dtp_config['output']['target_dir'], station_profile)

        try:
            yaml_output = yaml.dump(pygm_yaml['erddap'][station_profile])

            output.append(yaml_output)
            with open(file_name, 'w') as file_writer:
                file_writer.write(yaml_output)
            
        except Exception as ex:
            dtp_logger.exception('Yaml output failed for station: %s' % (station_profile), exc_info=ex)
            dtp_logger.debug('Dumping Station YAML: %s' % (pygm_yaml['erddap'][station_profile]))
            
    return output

def translate_to_xml(config, pygm_yaml):
    exec_cmd = "%s %s %s ".lstrip() % (
        config['output']['profile_language'], 
        config['output']['profile_generator'], 
        config['output']['additional_arguments']
    )
    
    dtp_logger.info("Translating to XML using: %s" % (exec_cmd))
    print("Translating to XML using: %s" % (exec_cmd))

    if config['output']['working_dir']:
        print("Changing Working Directory to %s" % (config['output']['working_dir']))
        os.chdir(config['output']['working_dir'])

    for index_label, station_profile in enumerate(pygm_yaml['erddap']):
        yaml_path = os.path.abspath("%s/%s.yml" % (config['output']['target_dir'], station_profile))
        
        print("YAML Path: %s" % (yaml_path))

        if os.path.isfile(yaml_path):
            print("YAML Path Valid.")
            out = subprocess.run(
                exec_cmd % (yaml_path), 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT
            )

            print("Output: %s" % (out.stdout))
        else:
            print("ERROR: YAML source file \"%s\" not found!  Check Working direcotry and target directory paths in configuration!" % (yaml_path))



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="use this configuration file", action="store")
    args = parser.parse_args()

    main(args)
