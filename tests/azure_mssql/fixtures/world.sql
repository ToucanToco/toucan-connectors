DROP TABLE IF EXISTS City;
CREATE TABLE City (
        ID int,
        Name varchar(35) NOT NULL DEFAULT '',
        CountryCode char(3) NOT NULL DEFAULT '',
        District varchar(20) NOT NULL DEFAULT '',
        Population int NOT NULL DEFAULT '0'
)

INSERT INTO City VALUES (1,'Kabul','AFG','Kabol',1780000);
INSERT INTO City VALUES (2,'Qandahar','AFG','Qandahar',237500);
INSERT INTO City VALUES (3,'Herat','AFG','Herat',186800);
INSERT INTO City VALUES (4,'Mazar-e-Sharif','AFG','Balkh',127800);
INSERT INTO City VALUES (5,'Amsterdam','NLD','Noord-Holland',731200);
INSERT INTO City VALUES (6,'Rotterdam','NLD','Zuid-Holland',593321);
INSERT INTO City VALUES (7,'Haag','NLD','Zuid-Holland',440900);
INSERT INTO City VALUES (8,'Utrecht','NLD','Utrecht',234323);
INSERT INTO City VALUES (9,'Eindhoven','NLD','Noord-Brabant',201843);
INSERT INTO City VALUES (10,'Tilburg','NLD','Noord-Brabant',193238);
INSERT INTO City VALUES (11,'Groningen','NLD','Groningen',172701);
INSERT INTO City VALUES (12,'Breda','NLD','Noord-Brabant',160398);
INSERT INTO City VALUES (13,'Apeldoorn','NLD','Gelderland',153491);
INSERT INTO City VALUES (14,'Nijmegen','NLD','Gelderland',152463);
INSERT INTO City VALUES (15,'Enschede','NLD','Overijssel',149544);
INSERT INTO City VALUES (16,'Haarlem','NLD','Noord-Holland',148772);
INSERT INTO City VALUES (17,'Almere','NLD','Flevoland',142465);
INSERT INTO City VALUES (18,'Arnhem','NLD','Gelderland',138020);
INSERT INTO City VALUES (19,'Zaanstad','NLD','Noord-Holland',135621);
INSERT INTO City VALUES (20,'�s-Hertogenbosch','NLD','Noord-Brabant',129170);
INSERT INTO City VALUES (21,'Amersfoort','NLD','Utrecht',126270);
INSERT INTO City VALUES (22,'Maastricht','NLD','Limburg',122087);
INSERT INTO City VALUES (23,'Dordrecht','NLD','Zuid-Holland',119811);
INSERT INTO City VALUES (24,'Leiden','NLD','Zuid-Holland',117196);
INSERT INTO City VALUES (25,'Haarlemmermeer','NLD','Noord-Holland',110722);
INSERT INTO City VALUES (26,'Zoetermeer','NLD','Zuid-Holland',110214);
INSERT INTO City VALUES (27,'Emmen','NLD','Drenthe',105853);
INSERT INTO City VALUES (28,'Zwolle','NLD','Overijssel',105819);
INSERT INTO City VALUES (29,'Ede','NLD','Gelderland',101574);
INSERT INTO City VALUES (30,'Delft','NLD','Zuid-Holland',95268);
INSERT INTO City VALUES (31,'Heerlen','NLD','Limburg',95052);
INSERT INTO City VALUES (32,'Alkmaar','NLD','Noord-Holland',92713);
INSERT INTO City VALUES (33,'Willemstad','ANT','Cura�ao',2345);
INSERT INTO City VALUES (34,'Tirana','ALB','Tirana',270000);
INSERT INTO City VALUES (35,'Alger','DZA','Alger',2168000);
INSERT INTO City VALUES (36,'Oran','DZA','Oran',609823);
INSERT INTO City VALUES (37,'Constantine','DZA','Constantine',443727);
INSERT INTO City VALUES (38,'Annaba','DZA','Annaba',222518);
INSERT INTO City VALUES (39,'Batna','DZA','Batna',183377);
INSERT INTO City VALUES (40,'S�tif','DZA','S�tif',179055);
INSERT INTO City VALUES (41,'Sidi Bel Abb�s','DZA','Sidi Bel Abb�s',153106);
INSERT INTO City VALUES (42,'Skikda','DZA','Skikda',128747);
INSERT INTO City VALUES (43,'Biskra','DZA','Biskra',128281);
INSERT INTO City VALUES (44,'Blida (el-Boulaida)','DZA','Blida',127284);
INSERT INTO City VALUES (45,'B�ja�a','DZA','B�ja�a',117162);
INSERT INTO City VALUES (46,'Mostaganem','DZA','Mostaganem',115212);
INSERT INTO City VALUES (47,'T�bessa','DZA','T�bessa',112007);
INSERT INTO City VALUES (48,'Tlemcen (Tilimsen)','DZA','Tlemcen',110242);
INSERT INTO City VALUES (49,'B�char','DZA','B�char',107311);
INSERT INTO City VALUES (50,'Tiaret','DZA','Tiaret',100118);
