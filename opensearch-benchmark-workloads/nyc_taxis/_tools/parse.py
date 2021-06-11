import json
import csv
import sys
import re

types = {}
for f in ["vendor_id","cab_color","payment_type","trip_type","rate_code_id","store_and_fwd_flag"]:
  types[f] = 'keyword'
for f in ["vendor_name"]:
  types[f] = 'text'
for f in ["passenger_count"]:
  types[f] = 'integer'
for f in ["pickup_location", "dropoff_location"]:
  types[f] = 'geo_point'
for f in ["trip_distance", "fare_amount", "surcharge", "mta_tax", "extra", "ehail_fee", "improvement_surcharge", "tip_amount", "tolls_amount", "total_amount"]:
  types[f] = 'scaled_float'
for f in ["pickup_datetime", "dropoff_datetime"]:
  types[f] = 'date'

def write_mappings():
  mappings = {}
  for (k, v) in types.items():
    mappings[k] = { "type": v }
    if v == 'date':
      mappings[k]['format'] = "yyyy-MM-dd HH:mm:ss"
    elif v == 'scaled_float':
      mappings[k]['scaling_factor'] = 100
  mappings = { "properties": mappings }
  mappings['_all'] = { "enabled": False }
  mappings['dynamic'] = 'strict'
  mappings = { "type": mappings }
  print(json.dumps(mappings, indent=2))

def to_geo_point(d, f):
  lat_field = f + "_latitude"
  lon_field = f + "_longitude"
  if lat_field in d and lon_field in d:
    longitude = float(d[lon_field])
    latitude = float(d[lat_field])
    if longitude < -180 or longitude > 180 or latitude < -90 or latitude > 90:
      raise Exception("Malformed coordinates")
    d[f + '_location'] = [float(d[lon_field]), float(d[lat_field])]
    del d[lon_field]
    del d[lat_field]

def to_underscore(s):
  s = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', s)
  return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s).lower()

def to_json(f):
  fields = []
  for field in f.readline().strip().split(','):
    field = to_underscore(field)
    if field.startswith('tpep_') or field.startswith('lpep_'):
      field = field[5:]
    elif field == 'ratecode_id':
      field = 'rate_code_id'
    fields.append(field)
  for line in f.readlines():
    cols = line.strip().split(',')
    if len(cols) < len(fields):
      raise Exception("Cannot parse '%s': number of fields does not match '%s'" %(line, ",".join(fields)))

    try:
      d = {}
      for i in range(len(fields)):
        field = fields[i]
        value = cols[i]
        if value != '': # the way csv says the field does not exist
          d[field] = value

      to_geo_point(d, 'pickup')
      to_geo_point(d, 'dropoff')

      for (k, v) in d.items():
        if k not in types:
          raise Exception("Unknown field '%s'" %k)
        t = types[k]
        try:
          if t == 'integer':
            d[k] = int(v)
          elif t == 'float':
            d[k] = float(v)
        except Exception as cause:
          raise Exception("Cannot parse (%s,%s)" %(k, v)) from cause

      print(json.dumps(d))
    except KeyboardInterrupt:
      break
    except Exception as e:
      print("Skipping malformed entry '%s' because of %s" %(line, str(e)), file=sys.stderr)

if sys.argv[1] == "json":
  for file_name in sys.argv[2:]:
    with open(file_name) as f:
      to_json(f)
elif sys.argv[1] == "mappings":
  write_mappings()
else:
  raise Exception("Expected 'json' or 'mappings' but got %s" %sys.argv[1])
