-----------------------------------------------------------------------
-- /* Se crea el campo geom para los centros provenientes del IDE */ --
-----------------------------------------------------------------------

-- Se crea una nueva columna

ALTER TABLE entradas.concesiones_acuicultura
ADD COLUMN geom geometry(Geometry, 102100);

-- Se pobla la nueva columna con la geometría basada en el campo 'geometry'

UPDATE entradas.concesiones_acuicultura
SET geom = ST_PolygonFromText(geometry, 102100);

-- Se elimina la columna que contiene la geometría como texto

ALTER TABLE entradas.concesiones_acuicultura
DROP COLUMN geometry;

