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
import validators

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
    if dtp_config["driver"]["driver_type"]:
        dtp_driver = load_driver(dtp_config)
        print(dtp_driver)
    else:
        dtp_driver = None

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

            sources = 0
            stations[id]["distribution"][sources]["name"] = "ERDDAP Data Subset Form"
            stations[id]["distribution"][sources]["url"] = escape(dataset_url)
            stations[id]["distribution"][sources]["description"]["en"] = "ERDDAP's version of the OPeNDAP .html web page for this dataset. Specify a subset of the dataset and download the data via OPeNDAP or in many different file types."
            stations[id]["distribution"][sources]["description"]["fr"] = "Version d'ERDDAP de la page Web OpenDAP .html pour ce jeu de données. Spécifiez un sous-ensemble du jeu de données et téléchargez les données via OpenDap ou dans de nombreux types de fichiers différents."
            
            if validators.url(row_series["infoUrl"]):
                sources = sources + 1
                stations[id]["distribution"].append({"url":"", "name":""})
                stations[id]["distribution"][sources]["url"] = row_series["infoUrl"]
                stations[id]["distribution"][sources]["name"] = "Information about dataset"

            if validators.url(row_series["sourceUrl"]):
                sources = sources + 1
                stations[id]["distribution"].append({"url":"", "name":""})
                stations[id]["distribution"][sources]["url"] = row_series["sourceUrl"]
                stations[id]["distribution"][sources]["name"] = "Original source of data"

            # create bounding box
            stations[id]["spatial"]["bbox"] = [
                row_series["minLongitude (degrees_east)"],
                row_series["minLatitude (degrees_north)"],
                row_series["maxLongitude (degrees_east)"],
                row_series["maxLatitude (degrees_north)"],
            ]

            # If date_published exists use that date, otherwise default to minTime (UTC)
            if row_series.get("date_published"):
                stations[id]["metadata"]["dates"]["publication"] = row_series["date_published"]
            else:
                stations[id]["metadata"]["dates"]["publication"] = row_series["minTime (UTC)"]

            stations[id]["metadata"]["dates"]["revision"] = row_series.get("date_revised")

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

        # if a depth min / max exists use those
        if erddap_meta(metadata, "depth_min")["value"] and erddap_meta(metadata, "depth_max")["value"]:
            station_data["spatial"]["vertical"] = [
                erddap_meta(metadata, "depth_min")["value"],
                erddap_meta(metadata, "depth_max")["value"],
            ]
        # if only a depth value exists set that as min/max
        elif erddap_meta(metadata, "depth")["value"]:
            station_data["spatial"]["vertical"] = [
                erddap_meta(metadata, "depth")["value"],
                erddap_meta(metadata, "depth")["value"],
            ]
        # invert altitude to be negative to match EPSG::5831 
        elif erddap_meta(metadata, "altitude")["value"]:
            station_data["spatial"]["vertical"] = [
                erddap_meta(metadata, "altitude")["value"] * -1,
                erddap_meta(metadata, "altitude")["value"] * -1,
            ]

        station_data["metadata"]["identifier"] = erddap_meta(metadata, "uuid")["value"]
        station_data["metadata"]["comment"] = erddap_meta(metadata, "comment")["value"]
        station_data["metadata"]["history"] = erddap_meta(metadata, "history")["value"]

        station_data["identification"]["title"]["en"] = erddap_meta(metadata, "title")["value"]
        station_data["identification"]["title"]["fr"] = erddap_meta(metadata, "title_fra")["value"]
        station_data["identification"]["abstract"]["en"] = erddap_meta(metadata, "summary")["value"]
        station_data["identification"]["abstract"]["fr"] = erddap_meta(metadata, "summary_fra")["value"]

        station_data["identification"]["dates"]["project"]["en"] = erddap_meta(metadata, "project")["value"]
        station_data["identification"]["dates"]["project"]["fr"] = erddap_meta(metadata, "project_fra")["value"]

        station_data["identification"]["acknowledgement"] = erddap_meta(metadata, "acknowledgement")["value"]

        if erddap_meta(metadata, "date_created")["value"]:
            station_data["identification"]["dates"]["creation"] = erddap_meta(metadata, "date_created")["value"]
        else:
            creation_date = datetime.strptime(erddap_meta(metadata, "time_coverage_start")["value"], "").strftime('%y-%m-%d')
            station_data["identification"]["dates"]["creation"] = creation_date

        if erddap_meta(metadata, "date_published")["value"]:
            station_data["identification"]["dates"]["publication"] = erddap_meta(metadata, "date_published")["value"]
        else:
            publication_date = datetime.strptime(erddap_meta(metadata, "time_coverage_start")["value"], "").strftime('%y-%m-%d')
            station_data["identification"]["dates"]["publication"] = publication_date

        # TODO: Contact Fields, expand to include https://wiki.esipfed.org/Attribute_Convention_for_Data_Discovery_1-2
        # and https://ioos.github.io/ioos-metadata/ioos-metadata-profile-v1-2.html

        # contributor_name, contributor_role
        contact_template = {
            "roles": [],
            "organization": {
                "name": "",
                "url": "",
                "address": "",
                "city": "",
                "country": "",
                "email": "",
            },
            "individual": {
                "name": "",
                "position": "",
                "email": "",
            }
        }

        contributor = copy.deepcopy(contact_template)

        contributor["roles"].append("contributor")
        contributor["organization"]["name"] = erddap_meta(metadata, "contributor_name")["value"]
        station_data["contact"].append(contributor)

        creator = copy.deepcopy(contact_template)

        creator["roles"].append("creator")
        if erddap_meta(metadata, "creator_type")["value"] == "institution":
            creator["organization"]["name"] = erddap_meta(metadata, "creator_name")["value"]
            creator["organization"]["email"] = erddap_meta(metadata, "creator_email")["value"]
            creator["organization"]["url"] = erddap_meta(metadata, "creator_url")["value"]

        station_data["contact"].append(creator)

        # publisher_institution, publisher_type, 
        publisher = copy.deepcopy(contact_template)

        publisher["roles"].append("creator")
        publisher["organization"]["name"] = erddap_meta(metadata, "publisher_name")["value"]
        publisher["organization"]["email"] = erddap_meta(metadata, "publisher_email")["value"]
        publisher["organization"]["country"] = erddap_meta(metadata, "publisher_country")["value"]
        publisher["organization"]["url"] = erddap_meta(metadata, "publisher_url")["value"]

        station_data["contact"].append(publisher)


        # ERDDAP ISO XML provides a list of dataset field names (long & short), data types & units
        # of measurement, in case this becomes useful for the CIOOS metadata standard we can extend
        # the YAML skeleton to include these and the template to export them.
        #
        # below most varible attributes from ERDDAP are extracted and pivoted to describe the field
        # actual field data types are extracted seperately and merged into the pivoted dataframe
        # for completeness
        # columns_pivot = metadata[
        #         (metadata["Variable Name"] != "NC_GLOBAL") & 
        #         (metadata["Row Type"] != "variable")
        #     ].pivot(index="Variable Name", columns="Attribute Name", values="Value")

        # col_data_types = metadata[(metadata["Row Type"] == "variable")][
        #     ["Variable Name", "Data Type"]
        # ]

        # df_merge = pd.merge(columns_pivot, col_data_types, on="Variable Name")

        # station_data["dataset"] = {}

        # TODO: UPDATE TO USE variable_1_x_blah notation
        # for index_label, field_series in df_merge.iterrows():
        #     field_name = field_series["Variable Name"]
        #     station_data["dataset"][field_name] = {}
        #     station_data["dataset"][field_name]["long_name"] = field_series["long_name"]
        #     station_data["dataset"][field_name]["data_type"] = field_series["Data Type"]
        #     station_data["dataset"][field_name]["units"] = field_series["units"]

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

                # if field in sanitize_fields:
                #     field_value = (
                #         field_value.replace(" > ", " &gt; ")
                #         .replace("/", "-")
                #         .replace("(", "")
                #         .replace(")", "")
                #     )

                station_data[field] = escape(field_value)
            except:
                dtp_logger.info(
                    "Field: %s in dataset %s not found in NC_GLOBAL.  Value: %s"
                    % (field, station_id, field_value)
                )

        # Remove duplicate keywords - mostly to account for duplicates added by repetitive GCMD entries
        for keyword_field in sanitize_fields:
            try:
                keywords = [
                    keyword.strip() for keyword in station_data[keyword_field].split(",")
                ]
                station_data[keyword_field] = ",".join(set(keywords))
            except KeyError:
                station_data[keyword_field] = None

        return_value = station_data

    return return_value


# Extracts data from the erddap metadata Pandas dataframe, NC_GLOBAL and
# row type attribute are assumed as defaults for variable specific values
# you'll need to specify those features
def erddap_meta(metadata, attribute_name, row_type="attribute", var_name="NC_GLOBAL"):
    # Example: uuid = metadata[(metadata['Variable Name']=='NC_GLOBAL') & (metadata['Attribute Name']=='uuid')]['Value'].values[0]
    return_value = {"value": None, "type": None}

    try:
        return_value["value"] = metadata[(metadata["Variable Name"] == var_name) & (metadata["Attribute Name"] == attribute_name)]["Value"].values[0]

        return_value["type"] = metadata[(metadata["Variable Name"] == var_name) & (metadata["Attribute Name"] == attribute_name)]["Data Type"].values[0]
    except IndexError:
        message = (
            "IndexError (Not found?) extracting ERDDAP Metadata: attribute: %s, row_type: %s, var_name: %s"
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
