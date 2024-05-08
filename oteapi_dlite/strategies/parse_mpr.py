"""Strategy that parses resource id and return all associated download links."""

import sys
from io import BytesIO
from pathlib import Path
from typing import Annotated, Optional

import dlite
import pandas as pd
import requests
from galvani import BioLogic as BL
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

        req = requests.get(
            str(config.downloadUrl),
            allow_redirects=True,
            timeout=(3, 27),
        )
        print("YAAAS")
        buffer = BytesIO(req.content)
        buffer.seek(0)
        mpr_file = BL.MPRfile(buffer)
        raw_data = pd.DataFrame(mpr_file.data)

        try:
            meta = get_meta(str(entity_uri))
        except Exception:
            # Retrieve and store entity locally
            req = requests.get(
                str(entity_uri),
                allow_redirects=True,
                timeout=(3, 27),
            )
            config_name = entity_uri.path.split("/").pop()
            Path(f"/entities/{config_name}").with_suffix(".json").write_bytes(
                req.content
            )

            meta = get_meta(str(entity_uri))

        # Create DLite instance
        inst = meta(dims=[len(raw_data)], id=config.id)

        relations = config.mpr_config

        print("In MPR parser")

        for relation_name, table_name in relations.items():
            print(relation_name, table_name)
            inst[relation_name] = raw_data[table_name]

        # Retrieve collection and add the entity instance
        coll = get_collection(collection_id=config.collection_id)
        coll.add(config.label, inst)
        update_collection(coll)

        return DLiteMPRSessionUpdate(
            collection_id=coll.uuid,
            inst_uuid=inst.uuid,
            label=config.label,
            mpr_data=raw_data.to_dict(),
        )
