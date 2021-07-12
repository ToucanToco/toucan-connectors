from toucan_connectors.query_manager import QueryManager

r1 = "SELECT SUM(population_2010) FROM communes;"
r2 = "SELECT COUNT(*) FROM communes WHERE num_departement=44;"
r3 = "SELECT nom, num_departement, surface FROM communes ORDER BY surface LIMIT 10;"
r4 = "SELECT nom, num_departement,population_2010 FROM communes ORDER BY population_2010 DESC LIMIT 10;"
r5 = "SELECT nom, population_2010/surface AS densité FROM communes WHERE num_departement=44 ORDER BY densité DESC LIMIT 12;"
r6 = "SELECT num_departement, COUNT(*) AS nb_communes FROM communes GROUP BY num_departement;"
r7 = "SELECT num_departement, SUM(population_2010) AS population FROM communes GROUP BY num_departement HAVING population>1000000 ORDER BY population DESC;"
r8 = "SELECT nom FROM communes WHERE nom LIKE ’%a%’ AND nom LIKE ’%e%’ AND nom LIKE ’%i%’ AND nom LIKE ’%o%’ AND nom LIKE ’%u%’ AND nom LIKE ’%y%’ ORDER BY LENGTH(nom);"
r9 = "SELECT nom, num_departement, zmax-zmin as écart FROM communes ORDER BY écart DESC LIMIT 15;"
r10 = "SELECT nom, population_2010-population_1999 AS var_abs FROM communes WHERE num_departement=44 ORDER BY var_abs DESC LIMIT 10;"
r11 = "SELECT nom, 1.*population_2010/population_1999-1 AS var_rel FROM communes WHERE num_departement=44 ORDER BY var_rel DESC LIMIT 10;"
r12 = "SELECT departements.nom, regions.nom FROM departements JOIN regions ON departements.num_region = regions.num_region AND (regions.nom = ’Bretagne’ OR regions.nom = ’Pays de la Loire’);"
r13 = "SELECT departements.nom, departements.num_departement, COUNT(*) as nb_communes, SUM(population_2010) AS population FROM communes JOIN departements ON communes.num_departement = departements.num_departement GROUP BY communes.num_departement ORDER BY population DESC;"
r14 = "SELECT regions.nom, SUM(population_2010)/SUM(surface) AS densité FROM communes JOIN departements ON communes.num_departement = departements.num_departement JOIN regions ON departements.num_region = regions.num_region GROUP BY regions.num_region ORDER BY densité DESC;"
r15 = "SELECT DISTINCT departements.nom FROM communes JOIN departements ON communes.num_departement = departements.num_departement WHERE communes.nom LIKE ’petit%’;"
r16 = "SELECT nom, population_2010 AS population FROM communes WHERE population > 100*(SELECT AVG(population_2010) FROM communes);"
r17 = "SELECT nom, num_departement, zmin FROM communes WHERE zmin = (SELECT MIN(zmin) FROM communes WHERE zmin >= 940 AND zmin != (SELECT MIN(zmin) FROM communes WHERE zmin >= 940));"
r18 = "SELECT DISTINCT regions.nom FROM departements JOIN regions ON departements.num_region = regions.num_region WHERE departements.nom LIKE ’V%’;"
r19 = "SELECT nom FROM regions WHERE EXISTS (SELECT * FROM departements WHERE departements.num_region = regions.num_region AND departements.nom LIKE ’V%’);"
r20 = "SELECT DISTINCT regions.nom AS nr FROM regions WHERE nr NOT IN (SELECT regions.nom FROM communes JOIN departements ON communes.num_departement = departements.num_departement JOIN regions ON departements.num_region = regions.num_region WHERE communes.nom LIKE ’%a%’ AND communes.nom LIKE ’%e%’ AND communes.nom LIKE ’%i%’ AND communes.nom LIKE ’%o%’ AND communes.nom LIKE ’%u%’ AND communes.nom LIKE ’%y%’);"
r21 = "SELECT nom FROM regions WHERE NOT EXISTS (SELECT * FROM communes JOIN departements ON communes.num_departement = departements.num_departement WHERE departements.num_region = regions.num_region AND communes.nom LIKE ’%a%’ AND communes.nom LIKE ’%e%’ AND communes.nom LIKE ’%i%’ AND communes.nom LIKE ’%o%’ AND communes.nom LIKE ’%u%’ AND communes.nom LIKE ’%y%’);"
r22 = "SELECT nom_région, communes.nom, population_2010 AS populationFROM (SELECT regions.nom AS nom_région, regions.num_region AS nr,MAX(population_2010) AS pop_max FROM communes JOIN departements ON communes.num_departement = departements.num_departement JOIN regions ON regions.num_region = departements.num_region GROUP BY regions.num_region) JOIN communes ON population_2010=pop_max ORDER BY population DESC;"


def test_prepare_query():
    print('')
    print(QueryManager().filter_request(query=r1, limit=10))
    # print(QueryManager().filter_request(query=r2, limit=10))
    # print(QueryManager().filter_request(query=r3, limit=10))
    # print(QueryManager().filter_request(query=r4, limit=10))
    # print(QueryManager().filter_request(query=r5, limit=10))
    # print(QueryManager().filter_request(query=r6, limit=10))
    # print(QueryManager().filter_request(query=r7, limit=10))
    # print(QueryManager().filter_request(query=r8, limit=10))
    # print(QueryManager().filter_request(query=r9, limit=10))
    # print(QueryManager().filter_request(query=r10, limit=10))
    # print(QueryManager().filter_request(query=r11, limit=10))
    # print(QueryManager().filter_request(query=r12, limit=10))
    # print(QueryManager().filter_request(query=r13, limit=10))
    # print(QueryManager().filter_request(query=r14, limit=10))
    # print(QueryManager().filter_request(query=r15, limit=10))
    # print(QueryManager().filter_request(query=r16, limit=10))
    # print(QueryManager().filter_request(query=r17, limit=10))
    # print(QueryManager().filter_request(query=r18, limit=10))
    # print(QueryManager().filter_request(query=r19, limit=10))
    # print(QueryManager().filter_request(query=r20, limit=10))
    print(QueryManager().filter_request(query=r21, limit=10))
    # print(QueryManager().filter_request(query=r22, limit=10))
