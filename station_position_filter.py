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

  lat_adjust = float(prog_args.lat_adjust)
  lon_adjust = float(prog_args.lon_adjust)
  
  with urllib.request.urlopen(stations_url) as url:
    stations = json.loads(url.read().decode())
      
  station_locs = {}
  for station_name in stations['stations']:
    station = stations['stations'][station_name]
    station_locs[station['erddap_id']] = station['location']
    print("%s %s %s" % (station_name, station['erddap_id'], station['location']))

  print(station_locs)
  print(config['output']['target_dir'])
  files = os.scandir(config['output']['target_dir'])

  for file in files:
    if file.name.endswith('yml'):
      load_path = "%s/%s" % (config['output']['target_dir'], file.name)

      with open(load_path) as f:
        yaml_file = yaml.safe_load(f)

      station_key = file.name.replace('.yml', '') # file name minus extension

      print("Station: %s" % (station_key))
      
      try:
        value = station_locs[station_key]
        
        print("Y Lat min %s" % (yaml_file['geospatial_lat_min']))
        print("Y Lat max %s" % (yaml_file['geospatial_lat_max']))
        print("Y Lon min %s" % (yaml_file['geospatial_lon_min']))
        print("Y Lon max %s" % (yaml_file['geospatial_lon_max']))

        yaml_file['geospatial_lat_min'] = value[0] + lat_adjust * -1
        yaml_file['geospatial_lat_max'] = value[0] + lat_adjust
        yaml_file['geospatial_lon_min'] = value[1] + lon_adjust * -1
        yaml_file['geospatial_lon_max'] = value[1] + lon_adjust

        with open(load_path, "w") as f:
          yaml.safe_dump(yaml_file, f)
      except KeyError as ex:
        print("YAML file '%s' does not match expected list of stations: %s" % (file.name, ex))


if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument("-c", "--config", help="use this configuration file", default='dtp_config.ini', action="store")
  parser.add_argument("-u", "--url", help="source url for json file", default='https://www.smartatlantic.ca/stations.json', action="store")
  parser.add_argument("--lat_adjust", help="how much to adjust min/max latitude by", default='0.02', action="store")
  parser.add_argument("--lon_adjust", help="how much to adjust min/max longitude by", default='0.02', action="store")

  args = parser.parse_args()

  config = configparser.ConfigParser()
  config.read(args.config)

  main(args, config)
