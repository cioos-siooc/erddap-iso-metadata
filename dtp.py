import argparse
import configparser
import copy
import importlib
import logging
import ssl
from xml.sax.saxutils import escape
import isodate
import validators
from pathlib import Path
import pandas as pd
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

    print("filtering null values...")
    filtered_data = stripper(data_source)

    print("output_yaml_source...")
    final_result = output_yaml_source(dtp_config, filtered_data)


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

def split_and_strip(source_string, delimiter=","):
    try:
        new_list = [string_part.strip() for string_part in source_string.split(delimiter)]
    except AttributeError:
        new_list = None

    return new_list


# loads data from data source via 'driver'
def load_data_source(config, driver):
    data = {}
    print("Loading baseline station data from ERDDAP...")
    data["erddap"] = load_data_from_erddap(config)

    include_list = split_and_strip(config["datasets"]["include"])
    exclude_list = split_and_strip(config["datasets"]["exclude"])

    if len(include_list) > 0:
        filtered_dataset_list = {}
        for dataset_id in include_list:
            filtered_dataset_list[dataset_id] = data["erddap"][dataset_id]

        data["erddap"] = filtered_dataset_list

    if len(exclude_list) > 0:
        for dataset_id in exclude_list:
            try:
                print("Removing '%s'" % (dataset_id))
                data["erddap"][dataset_id].pop()
            except KeyError:
                print("'%s' not found.  Skipping." % (dataset_id))

    print("Filtered List of Datasets:")
    print(data["erddap"].keys())

    for index_label, station_profile in enumerate(data["erddap"]):
        print("Loading Individual Profile: %s" % (station_profile))
        data["erddap"][station_profile] = load_data_from_erddap(
            config, station_profile, data["erddap"][station_profile]
        )

    # Harvest Station data from ERDDAP metadata, fields, units, etc.  Use OpenDAP .DAS feed
    # data['maintenance'] = driver.Driver.load_data(config)
    return data


def load_data_from_erddap(config, station_id=None, station_data=None):
    erddap_server = ERDDAP(
        server=config["dynamic_data"]["erddap_server"],
        protocol=config["dynamic_data"]["erddap_protocol"],
    )

    # eov_list = config['static_data']['eov_list'].split(',')

    if station_id is None:
        # load all station data MCF skeleton
        mcf_template = yaml.safe_load(open(config["static_data"]["mcf_template"], "r"))

        return_value = fetch_general_dataset_info(
            erddap_server=erddap_server, mcf_template=mcf_template
        )

    else:
        return_value = fetch_detailed_dataset_info(
            erddap_server=erddap_server,
            config=config,
            station_id=station_id,
            station_data=station_data,
        )

    return return_value


def fetch_general_dataset_info(erddap_server, mcf_template):
    stations = {}
    erddap_server.dataset_id = "allDatasets"

    # filter out "log in" datasets as the vast majoirty of their available metadata is unavailable
    erddap_server.constraints = {"accessible=": "public"}
    stations_df = erddap_server.to_pandas()

    # drop 'allDatasets' row
    stations_df.drop(labels=0, axis="index", inplace=True)
    # print(stations_df)

    for index_label, row_series in stations_df.iterrows():
        station_id = row_series["datasetID"]

        # ensure each station has an independant copy of the MCF skeleton
        stations[station_id] = copy.deepcopy(mcf_template)
        dataset_url = (
            row_series["tabledap"]
            if row_series["dataStructure"] == "table"
            else row_series["griddap"]
        )

        sources = 0
        stations[station_id]["distribution"][sources]["name"] = "ERDDAP Data Subset Form"
        stations[station_id]["distribution"][sources]["url"] = escape(dataset_url)
        stations[station_id]["distribution"][sources]["description"][
            "en"
        ] = "ERDDAP's version of the OPeNDAP .html web page for this dataset. Specify a subset of the dataset and download the data via OPeNDAP or in many different file types."
        stations[station_id]["distribution"][sources]["description"][
            "fr"
        ] = "Version d'ERDDAP de la page Web OpenDAP .html pour ce jeu de données. Spécifiez un sous-ensemble du jeu de données et téléchargez les données via OpenDap ou dans de nombreux types de fichiers différents."

        if validators.url(row_series["infoUrl"]):
            sources = sources + 1
            stations[station_id]["distribution"].append({"url": "", "name": ""})
            stations[station_id]["distribution"][sources]["url"] = row_series["infoUrl"]
            stations[station_id]["distribution"][sources]["name"] = "Information about dataset"

        if validators.url(row_series["sourceUrl"]):
            sources = sources + 1
            stations[station_id]["distribution"].append({"url": "", "name": ""})
            stations[station_id]["distribution"][sources]["url"] = row_series["sourceUrl"]
            stations[station_id]["distribution"][sources]["name"] = "Original source of data"

        # create bounding box
        stations[station_id]["spatial"]["bbox"] = [
            row_series["minLongitude (degrees_east)"],
            row_series["minLatitude (degrees_north)"],
            row_series["maxLongitude (degrees_east)"],
            row_series["maxLatitude (degrees_north)"],
        ]

        # If date_published exists use that date, otherwise default to minTime (UTC)
        if row_series.get("date_published"):
            stations[station_id]["metadata"]["dates"]["publication"] = row_series[
                "date_published"
            ]
        else:
            stations[station_id]["metadata"]["dates"]["publication"] = row_series[
                "minTime (UTC)"
            ]

        stations[station_id]["metadata"]["dates"]["revision"] = row_series.get("date_revised")

    return stations


def fetch_detailed_dataset_info(erddap_server, config, station_id, station_data):
    # load specific station data into MCF skeleton
    print(f"Loading ERDDAP metadata for station: {station_id}")

    # keywords_field = config['dynamic_data']['keywords_field']

    erddap_server.dataset_id = station_id

    metadata_url = erddap_server.get_download_url(
        dataset_id=f"{station_id}/index", response="csv", protocol="info"
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
    station_data["metadata"]["use_constraints"] = erddap_meta(metadata, "use_constraints")["value"]

    station_data["identification"]["title"]["en"] = erddap_meta(metadata, "title")["value"]
    station_data["identification"]["title"]["fr"] = erddap_meta(metadata, "title_fra")["value"]

    station_data["identification"]["abstract"]["en"] = erddap_meta(metadata, "summary")["value"]
    station_data["identification"]["abstract"]["fr"] = erddap_meta(metadata, "summary_fra")["value"]

    station_data["identification"]["project"]["en"] = split_and_strip(erddap_meta(metadata, "project")["value"])
    station_data["identification"]["project"]["fr"] = split_and_strip(erddap_meta(metadata, "project_fra")["value"])

    station_data["identification"]["keywords"]["default"]["en"] = split_and_strip(erddap_meta(metadata, "keywords")["value"])
    station_data["identification"]["keywords"]["default"]["fr"] = split_and_strip(erddap_meta(metadata, "keywords_fra")["value"])

    station_data["identification"]["keywords"]["eov"]["en"] = split_and_strip(erddap_meta(metadata, "cioos_eov")["value"])
    station_data["identification"]["keywords"]["eov"]["fr"] = split_and_strip(erddap_meta(metadata, "cioos_eov_fra")["value"])

    station_data["identification"]["acknowledgement"] = erddap_meta(metadata, "acknowledgement")["value"]
    station_data["identification"]["progress_code"] = erddap_meta(metadata, "progress_code")["value"]

    station_data["identification"]["status"] = erddap_meta(metadata, "status")["value"]
    station_data["identification"]["temporal_begin"] = erddap_meta(metadata, "time_coverage_start")["value"]
    station_data["identification"]["temporal_end"] = erddap_meta(metadata, "time_coverage_end")["value"]

    # calculate from temporal begin/end, duration format
    try:
        duration_begin = isodate.parse_datetime(
            station_data["identification"]["temporal_begin"]
        )
    except (TypeError, AttributeError):
        duration_begin = None

    try:
        duration_end = isodate.parse_datetime(
            station_data["identification"]["temporal_end"]
        )
    except (TypeError, AttributeError):
        duration_end = None

    try:
        station_data["identification"]["temporal_duration"] = isodate.duration_isoformat(duration_end - duration_begin)
    except (TypeError, AttributeError):
        station_data["identification"]["temporal_duration"] = None

    station_data["identification"]["time_coverage_resolution"] = erddap_meta(metadata, "time_coverage_resolution")["value"]

    # Date Created
    if erddap_meta(metadata, "date_created")["value"]:
        station_data["metadata"]["dates"]["creation"] = erddap_meta(metadata, "date_created")["value"]
        station_data["identification"]["dates"]["creation"] = erddap_meta(metadata, "date_created")["value"]
    else:
        try:
            creation_date = isodate.parse_datetime(erddap_meta(metadata, "time_coverage_start")["value"]).strftime("%Y-%m-%d")
        except (TypeError, AttributeError):
            creation_date = None

        station_data["metadata"]["dates"]["creation"] = creation_date
        station_data["identification"]["dates"]["creation"] = creation_date


    # Date Published
    if erddap_meta(metadata, "date_published")["value"]:
        station_data["identification"]["dates"]["publication"] = erddap_meta(metadata, "date_published")["value"]
    else:
        try:
            publication_date = isodate.parse_datetime(erddap_meta(metadata, "time_coverage_start")["value"]).strftime('%Y-%m-%d')
        except (TypeError, AttributeError):
            publication_date = None

        station_data["identification"]["dates"]["publication"] = publication_date

    # TODO: Contact Fields, expand to include https://wiki.esipfed.org/Attribute_Convention_for_Data_Discovery_1-2
    # and https://ioos.github.io/ioos-metadata/ioos-metadata-profile-v1-2.html
    contact_template = {
        "roles": [],
        "organization": {
            "name": "",
            "url": "",
            "address": "",
            "city": "",
            "country": "",
            "email": "",
            "phone": "",
        },
        "individual": {
            "name": "",
            "position": "",
            "email": "",
        },
    }

    # automate the creation and population of these fields using these two lists
    contact_roles = ["contributor", "creator", "publisher"]

    # added keys for more complete metadata generation:
    # - *_person_name,
    # - *_person_email,
    # - *_position,
    # - *_institution
    for role in contact_roles:
        contact = copy.deepcopy(contact_template)
        contact["roles"].append(role)

        type_key = erddap_meta(metadata, role + "_type")["value"]

        # if the type is not specified, it is assumed to be a person
        if not type_key:
            type_key = "person"

        # Common contact information regardless of role
        contact["organization"]["url"] = erddap_meta(metadata, role + "_url")["value"]
        contact["organization"]["address"] = erddap_meta(metadata, role + "_address")["value"]
        contact["organization"]["city"] = erddap_meta(metadata, role + "_city")["value"]
        contact["organization"]["country"] = erddap_meta(metadata, role + "_country")["value"]
        contact["organization"]["phone"] = erddap_meta(metadata, role + "_phone")["value"]

        contact["individual"]["position"] = erddap_meta(metadata, role + "_position")["value"]

        # contact information that shifts due to role and may require additional fields
        if any(role in type_key for role in ["person", "position"]):
            contact["individual"]["name"] = erddap_meta(metadata, role + "_name")["value"]
            contact["individual"]["email"] = erddap_meta(metadata, role + "_email")["value"]

            contact["organization"]["name"] = erddap_meta(metadata, role + "_institution")["value"]
        
        elif any(role in type_key for role in ["institution", "group"]):
            contact["organization"]["name"] = erddap_meta(metadata, role + "_name")["value"]
            contact["organization"]["email"] = erddap_meta(metadata, role + "_email")["value"]
            
            contact["individual"]["name"] = erddap_meta(metadata, role + "_person_name")["value"]
            contact["individual"]["email"] = erddap_meta(metadata, role + "_person_email")["value"]
            
        station_data["contact"].append(contact)

    # platform & instruments
    # platform_id *
    # platform_description *
    # platform_description_fra *
    # platform_description_eng *
    #
    # NOTE: both languages need not be specified, platform_description
    #       defaults to primary language, the secondary is automaticlly
    #       assumed to be the other
    platform_info = {
        "id": erddap_meta(metadata, "platform_id")["value"],
        "description": {
            "en": erddap_meta(metadata, "platform_description")["value"],
            "fr": erddap_meta(metadata, "platform_description_fra")["value"],
        },
        "instruments": [],
    }

    # New Keys (GLOBAL):
    # instrument_x_id *
    # instrument_x_manufacturer
    # instrument_x_version
    # instrument_x_type
    # instrument_x_type_fra
    # instrument_x_type_eng
    # instrument_x_description
    # instrument_x_description_fra
    # instrument_x_description_eng
    #
    # NOTE: Only instrument id is required
    instrument_template = {
        "id": "",
        "manufacturer": "",
        "version": "",
        "type": {
            "en": "",
            "fr": "",
        },
        "description": {
            "en": "",
            "fr": "",
        },
    }

    # add instruments to erddap profiles and fill them in here, similar
    # approach to contacts above.

    # create subset of instrument metadata, use regex to split and identify
    # instrument id and then iterate through possible fields, mapping them
    # to corresponding YAML fields.
    instruments = metadata[
        metadata["Attribute Name"].str.contains("^instrument_", na=False, regex=True)
    ]
    if not instruments.empty:
        # TODO: iterate through instruments
        pass

    station_data["platform"] = platform_info

    return station_data


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
            f"IndexError (Not found?) extracting ERDDAP Metadata: attribute: {attribute_name}, row_type: {row_type}, var_name: {var_name}"
        )
        dtp_logger.debug(message)

    return return_value

# https://stackoverflow.com/a/33529384/2112410
def stripper(data):
    new_data = {}
    for key, value in data.items():
        if isinstance(value, dict):
            value = stripper(value)

        if not value in ('', None, {}):
            new_data[key] = value

    return new_data

def output_yaml_source(dtp_config, filtered_data):
    output = []

    # TODO: use new config output settings and map to metadata-xml genereator
    for index_label, station_profile in enumerate(filtered_data["erddap"]):
        dtp_logger.info(f"Dumping YAML for {station_profile} profile")

        yaml_file_name = f"{dtp_config['output']['target_dir']}/{station_profile}.yml"
        xml_file_name = f"{dtp_config['output']['target_dir']}/{station_profile}.xml"

        output_path = Path(yaml_file_name)

        station_data = filtered_data["erddap"][station_profile]

        # If destination directory doesn't exist, create it
        if not output_path.parent.exists():
            Path.mkdir(output_path.parent, parents=True)

        yaml_output = yaml.dump(data=station_data, sort_keys=False)

        output.append(yaml_output)
        with open(yaml_file_name, "w", encoding="UTF-8") as file_writer:
            file_writer.write(yaml_output)

        xml_output = metadata_to_xml(station_data)
        with open(xml_file_name, "w", encoding="UTF-8") as file_writer:
            file_writer.write(xml_output)

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
