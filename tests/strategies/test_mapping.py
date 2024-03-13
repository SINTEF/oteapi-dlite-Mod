"""Tests mapping strategy."""
def test_mapping_without_prefixes() -> None:
    """Test without prefixes."""
    from tripper import EMMO, MAP, Namespace

    from oteapi_dlite.strategies.mapping import (
        DLiteMappingConfig,
        DLiteMappingStrategy,
    )
    from oteapi_dlite.utils import get_collection

    FORCES = Namespace("http://onto-ns.com/meta/0.1/Forces#")
    ENERGY = Namespace("http://onto-ns.com/meta/0.1/Energy#")

    config = DLiteMappingConfig(
        mappingType="mappings",
        triples=[
            (FORCES.forces, MAP.mapsTo, EMMO.Force),
            (ENERGY.energy, MAP.mapsTo, EMMO.PotentialEnergy),
        ],
    )

    mapper = DLiteMappingStrategy(config)
    session = mapper.initialize()
    session.update(mapper.get())

    collection = get_collection(session)
    relations = set((str(s), str(p), str(o)) for s, p, o in collection.get_relations())
    assert (str(FORCES.forces), str(MAP.mapsTo), str(EMMO.Force)) in relations
    assert (str(ENERGY.energy), str(MAP.mapsTo), str(EMMO.PotentialEnergy)) in relations


def test_mapping_with_prefixes() -> None:
    """Test with prefixes."""
    from tripper import EMMO, MAP, Namespace

    from oteapi_dlite.strategies.mapping import (
        DLiteMappingConfig,
        DLiteMappingStrategy,
    )
    from oteapi_dlite.utils import get_collection

    FORCES = Namespace("http://onto-ns.com/meta/0.1/Forces#")
    ENERGY = Namespace("http://onto-ns.com/meta/0.1/Energy#")

    config = DLiteMappingConfig(
        mappingType="mappings",
        prefixes={
            "f": "http://onto-ns.com/meta/0.1/Forces#",
            "e": "http://onto-ns.com/meta/0.1/Energy#",
        },
        triples=[
            (FORCES.forces, MAP.mapsTo, EMMO.Force),
            (ENERGY.energy, MAP.mapsTo, EMMO.PotentialEnergy),
        ],
    )

    mapper = DLiteMappingStrategy(config)
    session = mapper.initialize()
    session.update(mapper.get())

    collection = get_collection(session)
    relations = set((str(s), str(p), str(o)) for s, p, o in collection.get_relations())
    assert len(list(collection.get_relations())) == len(relations)
    assert (str(FORCES.forces), str(MAP.mapsTo), str(EMMO.Force)) in relations
    assert (str(ENERGY.energy), str(MAP.mapsTo), str(EMMO.PotentialEnergy)) in relations
