import os
import yaml
import json
import urllib.request
import configparser
import logging
import argparse

logging.basicConfig(filename='station_position_filter.log', level=logging.DEBUG)

logger = logging.getLogger(__name__)

# runs all the things
def main(prog_args, config):
  stations_url = prog_args.url

  print(config)

  with urllib.request.urlopen(stations_url) as url:
    stations = json.loads(url.read().decode())
      
  for station_name in stations['stations']:
    station = stations['stations'][station_name]
    print("%s %s %s" % (station_name, station['erddap_id'], station['location']))

  print(config['output']['target_dir'])
  files = os.scandir(config['output']['target_dir'])

  for file in files:
    load_path = "%s/%s" % (config['output']['target_dir'], file.name)
    yaml_file = yaml.safe_load(load_path)
    print(file)


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("-c", "--config", help="use this configuration file", default='dtp_config.ini', action="store")
  parser.add_argument("-u", "--url", help="source url for json file", default='https://www.smartatlantic.ca/stations.json', action="store")
  args = parser.parse_args()

  config = configparser.ConfigParser()
  config.read(args.config)

  main(args, config)
