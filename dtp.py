import argparse
import configparser
import copy
import importlib
import logging
import os
import re
import ssl
import subprocess
from datetime import datetime
from xml.sax.saxutils import escape  # use defusedxml instead

import pandas as pd
import pytz
import yaml
from erddapy import ERDDAP
from metadata_xml.template_functions import metadata_to_xml
from yamlinclude import YamlIncludeConstructor

ssl._create_default_https_context = ssl._create_unverified_context

logging.basicConfig(filename="dtp.log", level=logging.DEBUG)

dtp_logger = logging.getLogger(__name__)

# runs all the things
def main(prog_args):
    YamlIncludeConstructor.add_to_loader_class(loader_class=yaml.FullLoader)

    print("read_config...")
    dtp_config = read_config(prog_args)
    print(dtp_config)

    print("load_driver...")
    dtp_driver = load_driver(dtp_config)
    print(dtp_driver)

    print("load_data_source...")
    data_source = load_data_source(dtp_config, dtp_driver)
    # print(list(data_source))

    print("transform_data_source...")
    pygm_source = transform_data_source(dtp_config, data_source)
    # print(pygm_source)

    print("translate_into_yaml...")
    pygm_yaml = translate_into_yaml(dtp_config, pygm_source)
    # print(pygm_yaml)

    print("output_yaml_source...")
    final_result = output_yaml_source(dtp_config, pygm_yaml)
    # print(pygm_yaml)


# loads configuration data
def read_config(prog_args):
    config = configparser.ConfigParser()

    print("Using configuration: %s" % (prog_args.config))
    config.read(prog_args.config)

    return config


# load data source 'driver'
def load_driver(config):
    # not currently being used, will draw on later to merge in SharePoint data
    driver = importlib.import_module("drivers." + config["driver"]["driver_type"])
    return driver


# loads data from data source via 'driver'
def load_data_source(config, driver):
    data = {}
    print("Loading baseline station data from ERDDAP...")
    data["erddap"] = load_data_from_erddap(config)

    for index_label, station_profile in enumerate(data["erddap"]):
        print("Loading Individual Profile: %s" % (station_profile))
        data["erddap"][station_profile] = load_data_from_erddap(
            config, station_profile, data["erddap"][station_profile]
        )

    # Harvest Station data from ERDDAP metadata, fields, units, etc.  Use OpenDAP .DAS feed
    # data['maintenance'] = driver.Driver.load_data(config)
    return data


def load_data_from_erddap(config, station_id=None, station_data=None):
    mcf_template = yaml.safe_load(open(config["static_data"]["mcf_template"], "r"))

    es = ERDDAP(
        server=config["dynamic_data"]["erddap_server"],
        protocol=config["dynamic_data"]["erddap_protocol"],
    )

    # eov_list = config['static_data']['eov_list'].split(',')

    if station_id is None:
        # load all station data MCF skeleton
        stations = {}
        es.dataset_id = "allDatasets"

        # filter out "log in" datasets as the vast majoirty of their available metadata is unavailable
        es.constraints = {"accessible=": "public"}
        stations_df = es.to_pandas()

        # drop 'allDatasets' row
        stations_df.drop(labels=0, axis="index", inplace=True)
        # print(stations_df)

        for index_label, row_series in stations_df.iterrows():
            id = row_series["datasetID"]

            # ensure each station has an independant copy of the MCF skeleton
            stations[id] = copy.deepcopy(mcf_template)
            dataset_url = (
                row_series["tabledap"]
                if row_series["dataStructure"] == "table"
                else row_series["griddap"]
            )

            stations[id]["distribution"][0]["url"] = escape(dataset_url)

            # create bounding box
            stations[id]["spatial"]["bbox"] = [
                row_series["minLongitude (degrees_east)"],
                row_series["minLatitude (degrees_north)"],
                row_series["maxLongitude (degrees_east)"],
                row_series["maxLatitude (degrees_north)"],
            ]

            stations[id]["dates"]["creation"] = row_series["minTime (UTC)"]
            stations[id]["dates"]["lastUpdate"] = row_series["maxTime (UTC)"]

        return_value = stations

    else:
        # load specific station data into MCF skeleton
        print("Loading ERDDAP metadata for station: %s" % (station_id))

        # keywords_field = config['dynamic_data']['keywords_field']

        es.dataset_id = station_id

        metadata_url = es.get_download_url(
            dataset_id="%s/index" % (station_id), response="csv", protocol="info"
        )
        metadata = pd.read_csv(filepath_or_buffer=metadata_url)

        station_data["metadata"]["identifier"] = erddap_meta(metadata, "uuid")["value"]

        station_data["identification"]["title"]["en"] = erddap_meta(metadata, "title")[
            "value"
        ]
        station_data["identification"]["title"]["fr"] = erddap_meta(
            metadata, "title_fra"
        )["value"]
        station_data["identification"]["abstract"]["en"] = erddap_meta(
            metadata, "summary"
        )["value"]
        station_data["identification"]["abstract"]["fr"] = erddap_meta(
            metadata, "summary_fra"
        )["value"]

        # ERDDAP ISO XML provides a list of dataset field names (long & short), data types & units
        # of measurement, in case this becomes useful for the CIOOS metadata standard we can extend
        # the YAML skeleton to include these and the template to export them.
        #
        # below most varible attributes from ERDDAP are extracted and pivoted to describe the field
        # actual field data types are extracted seperately and merged into the pivoted dataframe
        # for completeness
        columns_pivot = metadata[
            (metadata["Variable Name"] != "NC_GLOBAL")
            & (metadata["Row Type"] != "variable")
        ].pivot(index="Variable Name", columns="Attribute Name", values="Value")
        col_data_types = metadata[(metadata["Row Type"] == "variable")][
            ["Variable Name", "Data Type"]
        ]
        df_merge = pd.merge(columns_pivot, col_data_types, on="Variable Name")

        station_data["dataset"] = {}

        # TODO: UPDATE TO USE variable_1_x_blah notation
        for index_label, field_series in df_merge.iterrows():
            field_name = field_series["Variable Name"]
            station_data["dataset"][field_name] = {}
            station_data["dataset"][field_name]["long_name"] = field_series["long_name"]
            station_data["dataset"][field_name]["data_type"] = field_series["Data Type"]
            station_data["dataset"][field_name]["units"] = field_series["units"]

        sanitize_fields = [
            keyword.strip()
            for keyword in config["static_data"]["sanitize_fields"].split(",")
        ]

        for index, field in enumerate(
            config["static_data"]["opt_rec_variables"].split(",")
        ):
            field = field.strip()

            try:
                field_value = erddap_meta(metadata, field)["value"]

                if field in sanitize_fields:
                    field_value = (
                        field_value.replace(" > ", ", ")
                        .replace("/", "-")
                        .replace("(", "")
                        .replace(")", "")
                    )

                station_data[field] = escape(field_value)
            except:
                dtp_logger.info(
                    "Field: %s in dataset %s not found in NC_GLOBAL.  Value: %s"
                    % (field, station_id, field_value)
                )

        # Remove duplicate keywords - mostly to account for duplicates added by repetitive GCMD entries
        for keyword_field in sanitize_fields:
            keywords = [
                keyword.strip() for keyword in station_data[keyword_field].split(",")
            ]
            station_data[keyword_field] = ",".join(set(keywords))

        return_value = station_data

    return return_value


# Extracts data from the erddap metadata Pandas dataframe, NC_GLOBAL and
# row type attribute are assumed as defaults for variable specific values
# you'll need to specify those features
def erddap_meta(metadata, attribute_name, row_type="attribute", var_name="NC_GLOBAL"):
    # Example: uuid = metadata[(metadata['Variable Name']=='NC_GLOBAL') & (metadata['Attribute Name']=='uuid')]['Value'].values[0]
    return_value = {"value": None, "type": None}

    try:
        return_value["value"] = metadata[
            (metadata["Variable Name"] == var_name)
            & (metadata["Attribute Name"] == attribute_name)
        ]["Value"].values[0]
        return_value["type"] = metadata[
            (metadata["Variable Name"] == var_name)
            & (metadata["Attribute Name"] == attribute_name)
        ]["Data Type"].values[0]
    except IndexError:
        message = (
            "IndexError extracting ERDDAP Metadata: attribute: %s, row_type: %s, var_name: %s"
            % (attribute_name, row_type, var_name)
        )
        dtp_logger.debug(message)

        # print(message)
        # print(metadata[(metadata['Variable Name']==var_name) & (metadata['Attribute Name']==attribute_name)]['Value'])
        # print(metadata[(metadata['Variable Name']==var_name) & (metadata['Attribute Name']==attribute_name)]['Data Type'])
        # print(metadata)

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
    for index_label, station_profile in enumerate(pygm_yaml["erddap"]):
        dtp_logger.info("Dumping YAML for %s profile" % (station_profile))

        file_name = "%s/%s.yml" % (dtp_config["output"]["target_dir"], station_profile)

        try:
            yaml_output = yaml.dump(pygm_yaml["erddap"][station_profile])

            output.append(yaml_output)
            with open(file_name, "w") as file_writer:
                file_writer.write(yaml_output)

        except Exception as ex:
            dtp_logger.exception(
                "Yaml output failed for station: %s" % (station_profile), exc_info=ex
            )
            dtp_logger.debug(
                "Dumping Station YAML: %s" % (pygm_yaml["erddap"][station_profile])
            )

    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        help="use this configuration file",
        default="dtp_config.ini",
        action="store",
    )
    args = parser.parse_args()

    main(args)
