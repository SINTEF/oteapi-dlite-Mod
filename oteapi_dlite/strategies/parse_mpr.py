"""Strategy that parses resource id and return all associated download links."""

import sys
from pathlib import Path
from typing import Annotated, Any, Optional

import dlite
import pandas as pd
import requests
from galvani import BioLogic as BL
from oteapi.datacache import DataCache
from oteapi.models import (
    AttrDict,
    DataCacheConfig,
    HostlessAnyUrl,
    ParserConfig,
    ResourceConfig,
)
from oteapi.plugins import create_strategy
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
    mediaType: Optional[str] = Field(
        None,
        description=ResourceConfig.model_fields["mediaType"].description,
    )
    storage_path: Annotated[
        Optional[str],
        Field(
            description="Path to metadata storage",
        ),
    ] = None
    collection_id: Annotated[
        Optional[str], Field(description="A reference to a DLite collection.")
    ] = None

    mpr_config: AttrDict = Field(
        AttrDict(),
        description="A list of column names.",
    )
    datacache_config: Optional[DataCacheConfig] = Field(
        None,
        description=(
            "Configurations for the data cache for storing the downloaded file "
            "content."
        ),
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
    mpr_data: Annotated[
        dict[str, Any],
        Field(
            description="Label of the new instance in the collection.",
        ),
    ]


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
        print("In MPR parser")

        config = self.parse_config.configuration
        entity_uri = self.parse_config.entity

        try:
            # Update dlite storage paths if provided
            if config.storage_path:
                for storage_path in config.storage_path.split("|"):
                    print(f"Adding storage path: {storage_path}")
                    dlite.storage_path.append(storage_path)
        except Exception as e:
            print(f"Error during update of DLite storage path: {e}")
            raise RuntimeError("Failed to update DLite storage path.") from e

        # Download MPR file through a download strategy
        downloader = create_strategy("download", config.model_dump())
        downloader_output = downloader.get()
        cache = DataCache(config.datacache_config)

        # Load MPR file
        with cache.getfile(downloader_output["key"], suffix=".mpr") as filepath:
            mpr_file = BL.MPRfile(str(filepath))

        mpr_data = pd.DataFrame(mpr_file.data)

        # Create DLite instance
        try:
            meta = get_meta(str(entity_uri))
        except Exception:
            # Retrieve and store entity locally
            response = requests.get(
                str(entity_uri),
                allow_redirects=True,
                timeout=(3, 27),
            )
            config_name = entity_uri.path.split("/").pop()
            Path(f"/entities/{config_name}").with_suffix(".json").write_bytes(
                response.content
            )

            meta = get_meta(str(entity_uri))

        inst = meta(dims=[len(mpr_data)], id=config.id)

        relations = config.mpr_config

        for relation_name, table_name in relations.items():
            print(relation_name, table_name)
            inst[relation_name] = mpr_data[table_name]

        # Add the entity instance to the session-specific DLite collection
        coll = get_collection(collection_id=config.collection_id)
        coll.add(config.label, inst)
        update_collection(coll)

        return DLiteMPRSessionUpdate(
            collection_id=coll.uuid,
            inst_uuid=inst.uuid,
            label=config.label,
            mpr_data=mpr_data.to_dict(),
        )
