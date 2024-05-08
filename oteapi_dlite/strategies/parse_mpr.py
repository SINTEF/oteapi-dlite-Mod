"""Strategy that parses resource id and return all associated download links."""

import sys
from typing import Annotated, Optional

import dlite
import pandas as pd
import requests
from galvani import BioLogic as BL
from oteapi.datacache import DataCache
from oteapi.models import AttrDict, HostlessAnyUrl, ParserConfig, ResourceConfig
from pydantic import Field
from pydantic.dataclasses import dataclass

from oteapi_dlite.models import DLiteSessionUpdate
from oteapi_dlite.utils import get_collection, get_meta, update_collection

if sys.version_info >= (3, 10):
    from typing import Literal
else:
    from typing_extensions import Literal


class DLiteMPRParseConfig(AttrDict):
    """MPR parse-specific Configuration Data Model."""

    id: Optional[str] = Field(None, description="Optional id on new instance.")

    label: Annotated[
        Optional[str],
        Field(
            description="Optional label for new instance in collection.",
        ),
    ] = "mpr-data"

    resourceType: Optional[Literal["resource/url"]] = Field(
        "resource/url",
        description=ResourceConfig.model_fields["resourceType"].description,
    )

    downloadUrl: Optional[HostlessAnyUrl] = Field(
        None,
        description=ResourceConfig.model_fields["downloadUrl"].description,
    )
    # entity: Optional[str] = Field(
    #     None,
    #     description="Name of the data model entity",
    # )
    mediaType: Optional[str] = Field(
        None,
        description=ResourceConfig.model_fields["mediaType"].description,
    )
    collection_id: Annotated[
        Optional[str], Field(description="A reference to a DLite collection.")
    ] = None

    mpr_config: AttrDict = Field(
        AttrDict(),
        description="A list of column names.",
    )


class DLiteMPRStrategyConfig(ParserConfig):
    """DLite mpr parse strategy  config."""

    configuration: Annotated[
        DLiteMPRParseConfig,
        Field(description="DLite mpr parse strategy-specific configuration."),
    ]


class DLiteMPRSessionUpdate(DLiteSessionUpdate):
    """Class for returning values from DLite mpr parser."""

    inst_uuid: Annotated[
        str,
        Field(
            description="UUID of new instance.",
        ),
    ]
    label: Annotated[
        str,
        Field(
            description="Label of the new instance in the collection.",
        ),
    ]
    # mpr_data: Annotated[
    #     AttrDict,
    #     Field(
    #         description="Label of the new instance in the collection.",
    #     ),
    # ]


@dataclass
class DLiteMPRStrategy:
    """Parse strategy for MPR.

    **Registers strategies**:

    - `("mediaType", "application/parse-mpr")`

    """

    parse_config: DLiteMPRStrategyConfig

    def initialize(self) -> DLiteSessionUpdate:
        """Initialize."""
        collection_id = (
            self.parse_config.configuration.collection_id
            or get_collection().uuid
        )
        return DLiteSessionUpdate(collection_id=collection_id)

    def get(self) -> DLiteMPRSessionUpdate:

        config = self.parse_config
        # try:
        #     # Update dlite storage paths if provided
        #     if config.storage_path:
        #         for storage_path in config.storage_path.split("|"):
        #             dlite.storage_path.append(storage_path)
        # except Exception as e:
        #     print(f"Error during update of DLite storage path: {e}")
        #     raise RuntimeError("Failed to update DLite storage path.") from e

        req = requests.get(
            str(config.configuration.downloadUrl),
            allow_redirects=True,
            timeout=(3, 27),
        )
        cache = DataCache()
        key = cache.add(req.content)

        with cache.getfile(key, suffix=".mpr") as filename:
            mpr_file = BL.MPRfile(str(filename))
        raw_data = pd.DataFrame(mpr_file.data)

        if config.entity:
            req = requests.get(
                str(config.entity),
                allow_redirects=True,
                timeout=(3, 27),
            )

            config_name = config.entity.path.split("/").pop()

            with open(f"/entities/{config_name}", "wb") as file:
                file.write(req.content)
            dlite.storage_path.append(f"/entities")

            meta = get_meta(config.entity)
            inst = meta(dims=[len(raw_data)], id=config.configuration.id)

            relations = config.configuration.mpr_config

            for relation_name, table_name in relations.items():
                print(relation_name, table_name)
                inst[relation_name] = raw_data[table_name]

            coll = get_collection(
                collection_id=config.configuration.collection_id
            )

            coll.add(config.configuration.label, inst)
            update_collection(coll)

        return DLiteMPRSessionUpdate(
            collection_id=coll.uuid,
            inst_uuid=inst.uuid,
            label=config.configuration.label,
            mpr_data=raw_data.to_dict(),
        )
