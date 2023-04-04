from molastic import core


def test_date_mapper_parse_date_math():
    mapper = core.DateMapper(sourcepath="", targetpath="", type="date")
    mapper.map_value("now-5m")
