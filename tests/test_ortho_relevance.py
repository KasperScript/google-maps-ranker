from src import config, pipeline


def test_ortho_relevance_affects_ranking():
    original_hints = list(config.ORTHO_QUERY_HINTS)
    config.ORTHO_QUERY_HINTS = ["orthodontist"]
    try:
        quality = 90.0
        transit_score = 80.0

        ortho_place = {
            "name": "Dental Clinic",
            "found_by": [{"query": "orthodontist"}],
        }
        generic_place = {
            "name": "Dental Clinic",
            "found_by": [{"query": "dentist"}],
        }

        ortho_relevance = pipeline.compute_ortho_relevance(ortho_place)
        generic_relevance = pipeline.compute_ortho_relevance(generic_place)

        ortho_final = pipeline.compute_final_score(quality, transit_score, ortho_relevance)
        generic_final = pipeline.compute_final_score(quality, transit_score, generic_relevance)

        assert ortho_final > generic_final
    finally:
        config.ORTHO_QUERY_HINTS = original_hints


def test_ortho_relevance_defaults_to_base_when_no_provenance():
    place = {"name": "Dental Clinic"}
    assert pipeline.compute_ortho_relevance(place) == 50.0
