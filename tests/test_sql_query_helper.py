# ruff: noqa: E501
from toucan_connectors.sql_query_helper import SqlQueryHelper

requests = [
    "SELECT SUM(population_2010) FROM communes;",
    "SELECT COUNT(*) FROM communes WHERE num_departement=44;",
    "SELECT nom, num_departement, surface FROM communes ORDER BY surface LIMIT 10;",
    "SELECT nom, num_departement,population_2010 FROM communes ORDER BY population_2010 DESC LIMIT 10;",
    "SELECT nom, population_2010/surface AS densité FROM communes WHERE num_departement=44 ORDER BY densité DESC LIMIT 12;",
    "SELECT num_departement, COUNT(*) AS nb_communes FROM communes GROUP BY num_departement;",
    "SELECT num_departement, SUM(population_2010) AS population FROM communes GROUP BY num_departement HAVING population>1000000 ORDER BY population DESC;",
    'SELECT nom FROM communes WHERE nom LIKE "%a%" AND nom LIKE "%e%" AND nom LIKE "%i%" AND nom LIKE "%o%" AND nom LIKE "%u%" AND nom LIKE "%y%" ORDER BY LENGTH(nom);',
    "SELECT nom, num_departement, zmax-zmin as écart FROM communes ORDER BY écart DESC LIMIT 15;",
    "SELECT nom, population_2010-population_1999 AS var_abs FROM communes WHERE num_departement=44 ORDER BY var_abs DESC LIMIT 10;",
    "SELECT nom, 1.*population_2010/population_1999-1 AS var_rel FROM communes WHERE num_departement=44 ORDER BY var_rel DESC LIMIT 10;",
    'SELECT departements.nom, regions.nom FROM departements JOIN regions ON departements.num_region = regions.num_region AND (regions.nom = "Bretagne" OR regions.nom = "Pays de la Loire");',
    "SELECT departements.nom, departements.num_departement, COUNT(*) as nb_communes, SUM(population_2010) AS population FROM communes JOIN departements ON communes.num_departement = departements.num_departement GROUP BY communes.num_departement ORDER BY population DESC;",
    "SELECT regions.nom, SUM(population_2010)/SUM(surface) AS densité FROM communes JOIN departements ON communes.num_departement = departements.num_departement JOIN regions ON departements.num_region = regions.num_region GROUP BY regions.num_region ORDER BY densité DESC;",
    'SELECT DISTINCT departements.nom FROM communes JOIN departements ON communes.num_departement = departements.num_departement WHERE communes.nom LIKE "petit%";',
    "SELECT nom, population_2010 AS population FROM communes WHERE population > 100*(SELECT AVG(population_2010) FROM communes);",
    "SELECT nom, num_departement, zmin FROM communes WHERE zmin = (SELECT MIN(zmin) FROM communes WHERE zmin >= 940 AND zmin != (SELECT MIN(zmin) FROM communes WHERE zmin >= 940));",
    'SELECT DISTINCT regions.nom FROM departements JOIN regions ON departements.num_region = regions.num_region WHERE departements.nom LIKE "V%";',
    'SELECT nom FROM regions WHERE EXISTS (SELECT * FROM departements WHERE departements.num_region = regions.num_region AND departements.nom LIKE "V%");',
    'SELECT DISTINCT regions.nom AS nr FROM regions WHERE nr NOT IN (SELECT regions.nom FROM communes JOIN departements ON communes.num_departement = departements.num_departement JOIN regions ON departements.num_region = regions.num_region WHERE communes.nom LIKE "%a%" AND communes.nom LIKE "%e%" AND communes.nom LIKE "%i%" AND communes.nom LIKE "%o%" AND communes.nom LIKE "%u%" AND communes.nom LIKE "%y%");',
    'SELECT nom FROM regions WHERE NOT EXISTS (SELECT * FROM communes JOIN departements ON communes.num_departement = departements.num_departement WHERE departements.num_region = regions.num_region AND communes.nom LIKE "%a%" AND communes.nom LIKE "%e%" AND communes.nom LIKE "%i%" AND communes.nom LIKE "%o%" AND communes.nom LIKE "%u%" AND communes.nom LIKE "%y%");',
    "SELECT nom_région, communes.nom, population_2010 AS populationFROM (SELECT regions.nom AS nom_région, regions.num_region AS nr,MAX(population_2010) AS pop_max FROM communes JOIN departements ON communes.num_departement = departements.num_departement JOIN regions ON regions.num_region = departements.num_region GROUP BY regions.num_region) JOIN communes ON population_2010=pop_max ORDER BY population DESC;",
]


def test_prepare_query_with_limit():
    for request in requests:
        new_request = SqlQueryHelper().prepare_limit_query(query_string=request, limit=10)
        assert f"SELECT * FROM ({request.replace(';', '')}) LIMIT 10;" == new_request[0]


def test_prepare_query_show():
    new_request = SqlQueryHelper().prepare_limit_query(query_string="show schemas", limit=10)
    assert "show schemas" == new_request[0]


def test_prepare_count_query():
    request_sum = "SELECT nom, num_departement, surface FROM communes ORDER BY surface LIMIT 10;"
    new_request_sum = SqlQueryHelper().prepare_count_query(query_string=request_sum)
    assert f"SELECT COUNT(*) AS TOTAL_ROWS FROM ({request_sum.replace(';', '')});" == new_request_sum[0]

    request_sum = "SELECT nom, num_departement,population_2010 FROM communes ORDER BY population_2010 DESC LIMIT 10;"
    new_request_sum = SqlQueryHelper().prepare_count_query(query_string=request_sum)
    assert f"SELECT COUNT(*) AS TOTAL_ROWS FROM ({request_sum.replace(';', '')});" == new_request_sum[0]

    request_sum = "SELECT nom, population_2010/surface AS densité FROM communes WHERE num_departement=44 ORDER BY densité DESC LIMIT 12;"
    new_request_sum = SqlQueryHelper().prepare_count_query(query_string=request_sum)
    assert f"SELECT COUNT(*) AS TOTAL_ROWS FROM ({request_sum.replace(';', '')});" == new_request_sum[0]


def test_extract_limit():
    request = "SELECT COUNT(*) FROM communes WHERE num_departement=44;"
    result = SqlQueryHelper().extract_limit(request)
    assert result is None

    request = "SELECT nom, num_departement, surface FROM communes ORDER BY surface LIMIT 10;"
    result = SqlQueryHelper().extract_limit(request)
    assert result == 10

    request = "SELECT nom, num_departement,population_2010 FROM communes ORDER BY population_2010 DESC LIMIT 10;"
    result = SqlQueryHelper().extract_limit(request)
    assert result == 10

    # TODO - update wrong test
    request = "SELECT * FROM (SELECT * FROM communes LIMIT 10)"
    result = SqlQueryHelper().extract_limit(request)
    assert result == 10

    request = "SELECT * FROM (SELECT * FROM communes LIMIT prout)"
    result = SqlQueryHelper().extract_limit(request)
    assert result is None

    request = "SELECT * FROM (SELECT * FROM communes LIMIT )"
    result = SqlQueryHelper().extract_limit(request)
    assert result is None


def test_extract_offset():
    request = "SELECT COUNT(*) FROM communes WHERE num_departement=44;"
    result = SqlQueryHelper().extract_offset(request)
    assert result is None

    request = "SELECT nom, num_departement, surface FROM communes ORDER BY surface LIMIT 10 OFFSET 20;"
    result = SqlQueryHelper().extract_offset(request)
    assert result == 20

    request = "SELECT nom, num_departement,population_2010 FROM communes ORDER BY population_2010 DESC LIMIT 10;"
    result = SqlQueryHelper().extract_offset(request)
    assert result is None

    # TODO - update wrong test
    request = "SELECT * FROM (SELECT * FROM communes LIMIT 10 OFFSET 10)"
    result = SqlQueryHelper().extract_offset(request)
    assert result == 10

    request = "SELECT * FROM (SELECT * FROM communes OFFSET prout)"
    result = SqlQueryHelper().extract_offset(request)
    assert result is None

    request = "SELECT * FROM (SELECT * FROM communes OFFSET)"
    result = SqlQueryHelper().extract_offset(request)
    assert result is None


def test_count_query_needed():
    request_select = "SELECT * FROM communes;"
    result = SqlQueryHelper().count_query_needed(request_select)
    assert result is True

    request_select = "SHOW DATABASES;"
    result = SqlQueryHelper().count_query_needed(request_select)
    assert result is False
