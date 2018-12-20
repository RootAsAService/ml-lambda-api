import json
from base_tiler import mercantile_tiler
from stac_tools import stac_item_base
import datetime



def create_stac_item(bucket_name,
                     item_key,
                     geometry,
                     task_id,
                     algorithm,
                     source_data,
                     zoom=17,
                     datetimestamp=datetime.datetime.now().isoformat(),
                     title=None):



    if title is None:
        title = "Task ID - {task_id}, {Algorithm} applied to {source_data}".format(
            task_id=task_id,
            Algorithm=algorithm,
            source_data=source_data
        )

    tile_list = mercantile_tiler(geometry, zoom=17)

    num_tiles = len(tile_list)


    stac_properties = stac_item_base.build_stac_properties(datetimestamp,
                                                           title,
                                                           ml_algorithm=algorithm,
                                                           derived_from=source_data)

    ## add status properties
    ##
    stac_properties.update({"number_tiles": num_tiles,
                            "status": "0/{num_tiles} tiles_processed".format(num_tiles)})



    self_url = "s3://{bucket_name}/{item_key}".format(bucket_name=bucket_name,
                                                      item_key=item_key)

    stac_links = stac_item_base.build_stac_links(self_url,
                     derived_from_url=source_data,
                     root_catalog_url=None,
                     parent_catalog_url=None,
                     algorithm_collection_url=None)




    stac_dict = stac_item_base.create_stac_item(id,
                                                geometry.wkt,
                                                stac_properties=stac_properties,
                                                stac_links=stac_links,
                                                stac_assets={}
                                                )




    stac_item_base.save_stac_item(stac_dict=stac_dict,
                                  stac_bucket=bucket_name,
                                  stac_key=item_key)



    return stac_dict