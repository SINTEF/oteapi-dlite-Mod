# """Generic generate strategy using DLite storage plugin."""

# from numpy import ndarray
# import requests
# import os
# from typing import TYPE_CHECKING, Annotated, Optional

# import json

# import dlite

# # pylint: disable=unused-argument,invalid-name
# import tempfile

# from oteapi.datacache import DataCache
# from oteapi.models import AttrDict, DataCacheConfig, FunctionConfig
# from pydantic import Field
# from pydantic.dataclasses import dataclass

# from oteapi_dlite.models import DLiteSessionUpdate
# from oteapi_dlite.utils import get_collection, get_driver, update_collection


# from tripper import Triplestore


# if TYPE_CHECKING:  # pragma: no cover
#     from typing import Any


# class DLiteStorageConfig(AttrDict):
#     """Configuration for a generic DLite storage filter.

#     The DLite storage driver to can be specified using either the `driver`
#     or `functionType` field.

#     Where the output should be written, is specified using either the
#     `location` or `datacache_config.accessKey` field.

#     Either `label` or `datamodel` should be provided.
#     """

#     driver: Annotated[
#         Optional[str],
#         Field(
#             description='Name of DLite driver (ex: "json").',
#         ),
#     ] = None
#     functionType: Annotated[
#         Optional[str],
#         Field(
#             description='Media type for DLite driver (ex: "application/json").',
#         ),
#     ] = None
#     options: Annotated[
#         Optional[str],
#         Field(
#             description=(
#                 "Comma-separated list of options passed to the DLite "
#                 "storage plugin."
#             ),
#         ),
#     ] = None
#     location: Annotated[
#         Optional[str],
#         Field(
#             description=(
#                 "Location of storage to write to.  If unset to store in data "
#                 "cache using the key provided with "
#                 "`datacache_config.accessKey` (defaults to 'generate_data')."
#             ),
#         ),
#     ] = None
#     label: Annotated[
#         Optional[str],
#         Field(
#             description=(
#                 "Label of DLite instance in the collection to serialise."
#             ),
#         ),
#     ] = None
#     datamodel: Annotated[
#         Optional[str],
#         Field(
#             description=(
#                 "URI to the datamodel of the new instance.  Needed when "
#                 "generating the instance from mappings.  Cannot be combined "
#                 "with `label`"
#             ),
#         ),
#     ] = None
#     store_collection: Annotated[
#         bool,
#         Field(
#             description="Whether to store the entire collection in the session "
#             "instead of a single instance.  Cannot be combined with `label` or "
#             "`datamodel`.",
#         ),
#     ] = False
#     store_collection_id: Annotated[
#         Optional[str],
#         Field(
#             description="Used together with `store_collection` If given, store "
#             "a copy of the collection with this id.",
#         ),
#     ] = None
#     allow_incomplete: Annotated[
#         Optional[bool],
#         Field(
#             description="Whether to allow incomplete property mappings.",
#         ),
#     ] = False
#     collection_id: Annotated[
#         Optional[str],
#         Field(
#             description=("ID of the collection to use."),
#         ),
#     ] = None
#     datacache_config: Annotated[
#         Optional[DataCacheConfig],
#         Field(
#             description="Configuration options for the local data cache.",
#         ),
#     ] = None

#     entity: str = Field(
#         None,
#         description="Name of the data model entity",
#     )
#     save: Optional[bool] = Field(
#         False,
#         description="Do the results have to be stored in a file?",
#     )



# class DLiteGenerateConfig(FunctionConfig):
#     """DLite generate strategy config."""

#     configuration: Annotated[
#         DLiteStorageConfig,
#         Field(description="DLite generate strategy-specific configuration."),
#     ]


# @dataclass
# class DLiteGenerateStrategy:
#     """Generic DLite generate strategy utilising DLite storage plugins.

#     **Registers strategies**:

#     - `("functionType", "application/vnd.dlite-generate")`

#     """

#     generate_config: DLiteGenerateConfig

#     def initialize(self) -> DLiteSessionUpdate:
#         """Initialize."""
#         collection_id = (
#             self.generate_config.configuration.collection_id
#             or get_collection().uuid
#         )
#         return DLiteSessionUpdate(collection_id=collection_id)

#     def get(self) -> DLiteSessionUpdate:
#         """Execute the strategy.

#         This method will be called through the strategy-specific endpoint
#         of the OTE-API Services.
#         Returns:
#             SessionUpdate instance.
#         """
#         config = self.generate_config.configuration

#         entity = config.entity

#         cacheconfig = config.datacache_config

#         driver = (
#             config.driver
#             if config.driver
#             else get_driver(
#                 mediaType=config.functionType,
#             )
#         )

#         coll = get_collection(collection_id=config.collection_id)

#         if config.datamodel:
#             instances = coll.get_instances(
#                 metaid=config.datamodel,
#                 property_mappings=True,
#                 allow_incomplete=config.allow_incomplete,
#             )
#             inst = next(instances)
#         elif config.label:
#             inst = coll[config.label]
#         elif config.store_collection:
#             if config.store_collection_id:
#                 inst = coll.copy(newid=config.store_collection_id)
#             else:
#                 inst = coll
#         else:  # fail if there are more instances
#             raise ValueError(
#                 "One of `label` or `datamodel` configurations should be given."
#             )
        

#         generator_relations = []
#         parser_relations = []
#         for r in coll.relations:
#             if entity in r.s:
#                 generator_relations.append(r)
#             else:
#                 parser_relations.append(r)
            

#         # SPLIT RELATIONS
#         existing_properties = list(inst.properties.keys())
#         g_properties = {}
#         properties = inst.properties
#         for g_r in generator_relations:
#             g_s = g_r.s
#             g_o = g_r.o

#             if entity in g_r.s:
#                 g_n = g_s.split("#").pop()

#                 exists = False
#                 for p_r in parser_relations:
#                     p_s = p_r.s
#                     p_o = p_r.o

#                     if p_s == g_s and p_o == g_o:
#                         continue

#                     if p_o == g_o:
#                         p_n = p_s.split("#").pop()
                        
#                         if p_n in existing_properties:
#                             exists = True
#                         break

#                 if exists:
#                     if isinstance(properties[p_n], ndarray):
#                         g_properties[g_n] = properties[p_n].tolist()
#                     else:
#                         g_properties[g_n] = properties[p_n]

#         if config.save:
#             if config.save and config.location:
#                 inst.save(driver, config.location, config.options)
#             else:
#                 if cacheconfig and cacheconfig.accessKey:
#                     key = cacheconfig.accessKey

#                 cache = DataCache()
#                 with tempfile.TemporaryDirectory() as tmpdir:
#                     inst.save(driver, f"{tmpdir}/data", config.options)
#                     with open(f"{tmpdir}/data", "rb") as f:
#                         cache.add(f.read(), key=key)

#         return DLiteSessionUpdate(
#             properties=g_properties,
#             collection_id=coll.uuid
#         )
