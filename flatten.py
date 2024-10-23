###########################################################################
#
#  Copyright 2024 Google LLC
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
###########################################################################

import re
import argparse
import json
import pprint
import textwrap

from time import sleep

from bqflow.util.discovery_to_bigquery import Discovery_To_BigQuery

CAMEL_TO_UNDERSCORE = re.compile(r'([a-z0-9])([A-Z])')


def field_name(path: str) -> str:
  exists = set()
  name = []
  words = CAMEL_TO_UNDERSCORE.sub(r'\1_\2', path).lower().replace('.', '_').replace('_id', '').replace('_ids', '').title().split('_')
  for word in words:
    if word not in exists:
      exists.add(word)
      name.append(word)
  return '_'.join(name)


def enum_common(enums: list, delimiter: str = '_') -> str:
  prefix = ''

  if len(enums) >= 0:
    depth = 1
    while depth:
      candidate = delimiter.join(enums[0].split(delimiter, depth)[:depth]) + delimiter
      left = len(candidate)
      print(depth, candidate, left)
      if all(enums[i-1][:left] == enums[i][:left] for i in range(1, len(enums))):
        prefix = candidate
        depth += 1
      else:
        depth = 0

  return prefix


def flatten_json(data: dict, prefix: str = '', array: str = '') -> None:
  paths = []
  arrays = []

  if isinstance(data, dict):

    # root
    if prefix == '':
      for key, value in data.items():
        new_prefix = f"{prefix}.{key}" if prefix else key
        child_paths, child_arrays = flatten_json(value, new_prefix, array)
        paths.extend(child_paths)
        arrays.extend(child_arrays)
      return paths, arrays

    # arrays for unnest operations
    if data.get('type') == "array":
      array = prefix
      arrays.append({ 'array':array, 'object':'object' in data['items'] })
      if 'object' in data['items']:
        child_paths, child_arrays = flatten_json(data['items'], prefix, array)
        paths.extend(child_paths)
        arrays.extend(child_arrays)
      else: 
        paths.append({ 'path':prefix, 'type':data['items'].get('format', data['items']['type']), 'array':array })
      return paths, arrays

    # dict
    if data.get('type') == "dict":
      for key, value in data['object'].items():
        new_prefix = f"{prefix}.{key}" if prefix else key
        child_paths, child_arrays = flatten_json(value, new_prefix, array)
        paths.extend(child_paths)
        arrays.extend(child_arrays)
      return paths, arrays
      
    # enums end a tree
    if 'enum' in data: 
      common = enum_common(data['enum'])
      for key in data['enum']:
        new_prefix = f"{prefix}.{key}"
        paths.append({ 'path':new_prefix, 'type':'enum', 'value':key[len(common):], 'array':array })
      return paths, arrays

    # simple type
    paths.append({ 'path':prefix, 'type':data.get('format', data['type']), 'array':array })

  return paths, arrays


if __name__ == '__main__':

  parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=textwrap.dedent("""\
    - python flatten.py -api dfareporting -version v4 -function Advertiser
  """))

  # get parameters
  parser.add_argument('-api', help='api to run, name of product api')
  parser.add_argument('-version', help='version of api')
  parser.add_argument('-function', help='function or resource to call in api')

  args = parser.parse_args()

  paths, arrays = flatten_json(Discovery_To_BigQuery(args.api, args.version).resource_json(args.function))
 
  for path in paths:
    print(path)

  for array in arrays:
    print(array)

  query = ['SELECT']
  fields = []

  for path in paths:
    name = field_name(path['path'])
    fields.append(name)

    if path['type'] == 'boolean':
      query.append(f'  COUNTIF({path["path"]}) AS {name},')

    elif path['type'] in ('string', 'int32', 'int64'):
      if path['array']:
        query.append(f'  SUM(ARRAY_LENGTH({path["array"]})) AS {name},')
      else:
        query.append(f'  COUNT(DISTINCT {path["path"]}) AS {name},')

    elif path['type'] == 'enum':
      field, value = path['path'].rsplit('.', 1)
      if path['array']:
        struct = field.replace(path['array'] + '.', '')
        query.append(f'  SUM((SELECT COUNT({struct}) FROM UNNEST({path["array"]}) WHERE {struct} = "{value}")) AS {name},')
      else:
        query.append(f'  COUNTIF({field} = "{value}") AS {name},')

    elif path['type'] == 'google-datetime':
      for age in (7, 30, 90, 180, 365):
        query.append(f'  COUNTIF(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP({path["path"]}), DAY)  BETWEEN 0 AND 30) AS {name}_{age},')

    else:
      print('SKIPPED', path)

  print('\n'.join(query))

  print('UNPIVOT(value FOR attribute IN ({fields}))'.format(fields=', '.join(fields)))
