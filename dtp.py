import os, sys, getopt
from datetime import datetime
import pytz
import importlib
import re
import yaml
import pydap
import pandas as pd
from erddapy import ERDDAP

from yamlinclude import YamlIncludeConstructor
from pygeometa.core import render_template

import configparser
import logging
import argparse

logging.basicConfig(filename='dtp.log', level=logging.DEBUG)

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
    pass

# load data source 'driver'
def load_driver(config):
    # not currently being used, will draw on later to merge in SharePoint data
    driver = importlib.import_module('drivers.' + config['driver']['driver_type'])
    return driver
    pass

# loads data from data source via 'driver'
def load_data_source(config, driver):
    data = {}
    data['erddap'] = load_data_from_erddap(config)

    for index_label, station_profile in enumerate(data['erddap']):
        data['erddap'][station_profile] = load_data_from_erddap(config, station_profile, data['erddap'][station_profile])
        
    
    # Harvest Station data from ERDDAP metadata, fields, units, etc.  Use OpenDAP .DAS feed
    #data['maintenance'] = driver.Driver.load_data(config)
    return data
    pass

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
        stations_df = es.to_pandas()

        # drop 'allDatasets' row
        stations_df.drop(labels=0, axis='index', inplace=True)
        print(stations_df)

        for index_label, row_series in stations_df.iterrows():
            id = row_series['datasetID']
            
            stations[id] = mcf_template
            dataset_url = row_series['tabledap'] if row_series['dataStructure'] == 'table' else row_series['griddap']

            stations[id]['metadata']['identifier'] = id
            stations[id]['metadata']['dataseturi'] = dataset_url

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

        # should be able to layer this in later to describe dataset variables
        # a pivot should made to gather variable attributes into a table, indexed on column name
        columns = metadata[(metadata['Row Type']=='variable')]['Variable Name'].values

        station_data['identification']['keywords']['default']['keywords'] = metadata[(metadata['Variable Name']=='NC_GLOBAL') & (metadata['Attribute Name']=='keywords')]['Value'].values[0]

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
        file_name = '%s/%s.xml' % (dtp_config['output']['target_dir'], station_profile)

        iso_xml = render_template(pygm_yaml['erddap'][station_profile], schema_local=dtp_config['output']['target_schema'])

        with open(file_name, 'w') as file_writer:
            file_writer.write(iso_xml)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="use this configuration file", action="store")
    args = parser.parse_args()

    main(args)