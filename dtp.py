import os, sys, getopt
from datetime import datetime
import pytz
import importlib
import re
import yaml
import pydap
import pandas as pd
import copy
from erddapy import ERDDAP

from yamlinclude import YamlIncludeConstructor
from pygeometa.core import render_template

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
    print(list(data_source))

    print('transform_data_source...')
    pygm_source = transform_data_source(dtp_config, data_source)
    print(pygm_source)

    print('translate_into_yaml...')
    pygm_yaml = translate_into_yaml(dtp_config, pygm_source)
    print(pygm_yaml)

    print('process_info_schema...')
    metadata_formatted = process_info_schema(dtp_config, pygm_yaml)
    print(metadata_formatted)

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
    mcf_template = yaml.load(open(config['static_data']['mcf_template'], 'r'), Loader=yaml.FullLoader)

    es = ERDDAP(
        server=config['dynamic_data']['erddap_server'],
        protocol=config['dynamic_data']['erddap_protocol'],
    )

    if station_id is None:
        #load all station data MCF skeleton
        stations = {}
        es.dataset_id = 'allDatasets'

        # filter out "log in" datasets as the vast majoirty of their available metadata is unavailable
        es.constraints = {'accessible=': 'public'}
        stations_df = es.to_pandas()

        # drop 'allDatasets' row
        stations_df.drop(labels=0, axis='index', inplace=True)
        print(stations_df)

        for index_label, row_series in stations_df.iterrows():
            id = row_series['datasetID']
            
            # ensure each station has an independant copy of the MCF skeleton
            stations[id] = copy.deepcopy(mcf_template)
            dataset_url = row_series['tabledap'] if row_series['dataStructure'] == 'table' else row_series['griddap']

            stations[id]['metadata']['identifier'] = id
            stations[id]['metadata']['dataseturi'] = dataset_url
            
            stations[id]['spatial']['datatype'] = 'textTable' if row_series['dataStructure'] == 'table' else 'grid'

            stations[id]['spatial']['geomtype'] = row_series['cdm_data_type']
            stations[id]['spatial']['bbox'] = '%s,%s,%s,%s' % (row_series['minLongitude (degrees_east)'], row_series['minLatitude (degrees_north)'], row_series['maxLongitude (degrees_east)'], row_series['maxLatitude (degrees_north)'])

            stations[id]['identification']['title'] = row_series['title']
            stations[id]['identification']['dates']['creation'] = row_series['minTime (UTC)']
            stations[id]['identification']['temporal_begin'] = row_series['minTime (UTC)']
            stations[id]['identification']['temporal_end'] = row_series['maxTime (UTC)']
            stations[id]['identification']['url'] = dataset_url
            stations[id]['identification']['abstract'] = row_series['summary']

            stations[id]['distribution']['erddap']['url'] = dataset_url
            stations[id]['distribution']['erddap']['name'] = row_series['title']


        print('Stations after ERDDAP call...')
        print(stations)

        return_value = stations
        pass

    else:
        #load specific station data into MCF skeleton
        print('Loading ERDDAP metadata for station: %s' % (station_id))

        es.dataset_id = station_id

        metadata_url = es.get_download_url(dataset_id='%s/index' % (station_id), response='csv', protocol='info')
        metadata = pd.read_csv(filepath_or_buffer=metadata_url)
        print(metadata_url)
        print(metadata.head())

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
        
        for index_label, field_series in df_merge.iterrows():
            field_name = field_series['Variable Name']
            station_data['dataset'][field_name] = {}
            station_data['dataset'][field_name]['long_name'] = field_series['long_name']
            station_data['dataset'][field_name]['data_type'] = field_series['Data Type']
            station_data['dataset'][field_name]['units'] = field_series['units']

        station_data['identification']['keywords']['default']['keywords'] = metadata[(metadata['Variable Name']=='NC_GLOBAL') & (metadata['Attribute Name']=='keywords')]['Value'].values

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

# process pyYaml output into ISO metadata via pyGeometa
# https://github.com/geopython/pygeometa#using-the-api-from-python
def process_info_schema(dtp_config, pygm_yaml):
    for index_label, station_profile in enumerate(pygm_yaml['erddap']):
        dtp_logger.info('Processing YAML for %s' % (station_profile))

        file_name = '%s/%s.xml' % (dtp_config['output']['target_dir'], station_profile)

        try:
            iso_xml = render_template(pygm_yaml['erddap'][station_profile], schema_local=dtp_config['output']['target_schema'])

            with open(file_name, 'w') as file_writer:
                file_writer.write(iso_xml)
            
        except Exception as ex:
            dtp_logger.exception('PyGeoMeta render_template() failed for station: %s' % (station_profile), exc_info=ex)
            dtp_logger.debug('Dumping Station YAML: %s' % (yaml.dump(pygm_yaml['erddap'][station_profile])))



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="use this configuration file", action="store")
    args = parser.parse_args()

    main(args)
