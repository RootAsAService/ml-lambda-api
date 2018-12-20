"""app.main: handle request for lambda-tiler"""

import re
import json
import os
import urllib.request
import numpy as np
from rasterio import features
from rio_tiler import main
from rio_tiler.utils import (array_to_img,
                             linear_rescale,
                             get_colormap,
                             expression,
                             b64_encode_img)

import stac_tools
import s3_tools


from lambda_proxy.proxy import API
from distutils import util


APP = API(app_name="ml-lambda-api")


bucket_name = os.environ['stac_bucket_name']
default_algorithm = os.environ['default_algorithm']



@APP.route('/ml_start/', methods=['GET'], cors=True)
def ml_start():
    """Handle ml_start requests."""

    query_args = APP.current_request.query_params
    query_args = query_args if isinstance(query_args, dict) else {}

    geometry = query_args['geometry']
    task_id = query_args.get('task_id')
    algorithm = query_args.get('algorithm', 'default')
    source_data = query_args.get('source', 'default')


    ## create item_address:

    item_key = "{task_id}/{algorithm}.json".format(task_id=task_id, algorithm=algorithm)
    ## check if item exists:

    stac_contents = s3_tools.read_stac_item(bucket_name, item_key)
    if stac_contents:
        return ('OK', 'application/json', json.dumps(stac_contents))

    else:
        stac_contents = stac_tools.create_stac_item(bucket_name,
                                    item_key,
                                    geometry,
                                    task_id,
                                    algorithm,
                                    source_data)

        return ('OK', 'application/json', json.dumps(stac_contents))













    return ('OK', 'application/json', json.dumps({"geometry": data['geometry']}))




@APP.route('/stac/bounds', methods=['GET'], cors=True)
def bounds():
    """Handle bounds requests."""
    query_args = APP.current_request.query_params
    query_args = query_args if isinstance(query_args, dict) else {}
    address = query_args['url']

    with urllib.request.urlopen(address) as url:
        data = json.loads(url.read().decode())


    return ('OK', 'application/json', json.dumps({"geometry": data['geometry']}))

@APP.route('/stac/info', methods=['GET'], cors=True)
def bounds():
    """Handle bounds requests."""
    query_args = APP.current_request.query_params
    query_args = query_args if isinstance(query_args, dict) else {}
    address = query_args['url']

    with urllib.request.urlopen(address) as url:
        data = json.loads(url.read().decode())


    return ('OK', 'application/json', json.dumps(data))


@APP.route('/stac/tiles/<int:z>/<int:x>/<int:y>.<ext>', methods=['GET'], cors=True)
def tile(tile_z, tile_x, tile_y, tileformat):
    """Handle tile requests."""
    if tileformat == 'jpg':
        tileformat = 'jpeg'


    query_args = APP.current_request.query_params
    query_args = query_args if isinstance(query_args, dict) else {}

    address = query_args['url']

    ## Read Stac File
    with urllib.request.urlopen(address) as url:
        data = json.loads(url.read().decode())


    asset_key = query_args.get('asset_key', 'raster')

    raster_address = data['assets'][asset_key]['href']


    indexes = query_args.get('indexes')
    if indexes:
        indexes = tuple(int(s) for s in re.findall(r'\d+', indexes))

    tilesize = query_args.get('tile', 512)
    tilesize = int(tilesize) if isinstance(tilesize, str) else tilesize



    nodata = query_args.get('nodata')
    if nodata is not None:
        nodata = int(nodata)

    raster_address = data['assets'][asset_key]['href']

    tile, mask = main.tile(raster_address,
                           tile_x,
                           tile_y,
                           tile_z,
                           indexes=indexes,
                           tilesize=tilesize,
                           nodata=nodata)

    linearStretch = query_args.get('linearStretch')
    if linearStretch is not None:
        if util.strtobool(linearStretch):
            tile = linear_rescale(tile,
                                  in_range=(np.min(tile), np.max(tile))
                                  )

    img = array_to_img(tile, mask=mask)
    str_img = b64_encode_img(img, tileformat)

    return ('OK', f'image/{tileformat}', str_img)


@APP.route('/stac/summary/<int:z>/<int:x>/<int:y>.<ext>', methods=['GET'], cors=True)
def tile(tile_z, tile_x, tile_y, tileformat):
    """Handle tile requests."""
    if tileformat == 'jpg':
        tileformat = 'jpeg'


    query_args = APP.current_request.query_params
    query_args = query_args if isinstance(query_args, dict) else {}

    address = query_args['url']

    ## Read Stac File
    with urllib.request.urlopen(address) as url:
        data = json.loads(url.read().decode())


    asset_key = query_args.get('asset_key', 'raster')

    asset_data = data['assets'][asset_key]

    raster_address = asset_data['href']

    indexes = query_args.get('indexes')
    if indexes:
        indexes = tuple(int(s) for s in re.findall(r'\d+', indexes))

    tilesize = query_args.get('tile', 512)
    tilesize = int(tilesize) if isinstance(tilesize, str) else tilesize



    nodata = query_args.get('nodata')
    if nodata is not None:
        nodata = int(nodata)

    tile, mask = main.tile(raster_address,
                           tile_x,
                           tile_y,
                           tile_z,
                           indexes=indexes,
                           tilesize=tilesize,
                           nodata=nodata)

    tile_mask = tile >= asset_data['binary_threshold']

    if tilesize == 512:
        asset_data['pixel_count'] = np.sum(tile_mask[0])/2
    else:
        asset_data['pixel_count'] = np.sum(tile_mask[0])


    geom_list = []
    # Extract feature shapes and values from the array.
    for geom, val in features.shapes(tile_mask[0].astype(np.uint8), mask=mask):

        geom_list.append(geom)

    asset_data['object_count'] = len(geom_list)
    asset_data['object_list'] = geom_list



    return ('OK', 'application/json', json.dumps(asset_data))


@APP.route('/bounds', methods=['GET'], cors=True)
def bounds():
    """Handle bounds requests."""
    query_args = APP.current_request.query_params
    query_args = query_args if isinstance(query_args, dict) else {}
    address = query_args['url']
    info = main.bounds(address)
    return ('OK', 'application/json', json.dumps(info))


@APP.route('/tiles/<int:z>/<int:x>/<int:y>.<ext>', methods=['GET'], cors=True)
def tile(tile_z, tile_x, tile_y, tileformat):
    """Handle tile requests."""
    if tileformat == 'jpg':
        tileformat = 'jpeg'

    query_args = APP.current_request.query_params
    query_args = query_args if isinstance(query_args, dict) else {}

    address = query_args['url']

    indexes = query_args.get('indexes')
    if indexes:
        indexes = tuple(int(s) for s in re.findall(r'\d+', indexes))

    tilesize = query_args.get('tile', 512)
    tilesize = int(tilesize) if isinstance(tilesize, str) else tilesize

    nodata = query_args.get('nodata')
    if nodata is not None:
        nodata = int(nodata)

    tile, mask = main.tile(address,
                           tile_x,
                           tile_y,
                           tile_z,
                           indexes=indexes,
                           tilesize=tilesize,
                           nodata=nodata)

    linearStretch = query_args.get('linearStretch')
    if linearStretch is not None:
        if util.strtobool(linearStretch):
            tile = linear_rescale(tile,
                           in_range=(np.min(tile), np.max(tile))
                           )

    img = array_to_img(tile, mask=mask)
    str_img = b64_encode_img(img, tileformat)

    return ('OK', f'image/{tileformat}', str_img)


@APP.route('/processing/<int:z>/<int:x>/<int:y>.<ext>', methods=['GET'], cors=True)
def ratio(tile_z, tile_x, tile_y, tileformat):
    """Handle processing requests."""
    if tileformat == 'jpg':
        tileformat = 'jpeg'

    query_args = APP.current_request.query_params
    query_args = query_args if isinstance(query_args, dict) else {}

    address = query_args['url']

    ratio_value = query_args['ratio']

    range_value = query_args.get('range', [-1, 1])

    tilesize = query_args.get('tile', 512)
    tilesize = int(tilesize) if isinstance(tilesize, str) else tilesize

    tile, mask = expression(address,
                            tile_x,
                            tile_y,
                            tile_z,
                            ratio_value,
                            tilesize=tilesize)

    if len(tile.shape) == 2:
        tile = np.expand_dims(tile, axis=0)

    rtile = np.where(mask,
                     linear_rescale(tile, in_range=range_value, out_range=[0, 255]),
                     0).astype(np.uint8)

    img = array_to_img(rtile, color_map=get_colormap(name='cfastie'), mask=mask)
    str_img = b64_encode_img(img, tileformat)
    return ('OK', f'image/{tileformat}', str_img)


@APP.route('/favicon.ico', methods=['GET'], cors=True)
def favicon():
    """Favicon."""
    return('NOK', 'text/plain', '')
