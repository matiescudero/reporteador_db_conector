CREATE EXTENSION IF NOT EXISTS postgis;

-----------------------------------
---- PROCESAMIENTO INFORMACIÓN ----
-----------------------------------

----------------------------------
-- /* 1. TABLAS REPORTEADOR */ ---
----------------------------------

-----------------------------------------
-- /* 1.1 Concesiones Acuicultura */ ----
-----------------------------------------

/*
Se genera una vista que contiene la geometría de los distintos centros de cultivo de la décima región
y se reproyecta al SRID 4326. Se establece además si estos son centros son PSMB o no.
*/

DROP TABLE IF EXISTS capas_estaticas.centros_acuicultura;

CREATE TABLE capas_estaticas.centros_acuicultura AS (SELECT shp."REP_SUBPESCA2.ADM_UOT.PULLINQUE4_T_ACUICULTURA.N_CODIGOCENTRO" as codigocentro,
										  centros."Código Área" AS codigoarea,
										  shp.geom AS geom,
										  shp."REP_SUBPESCA2.ADM_UOT.PULLINQUE4_T_ACUICULTURA.TITULAR" AS titular,
										  centros."Nombre Sector" AS nombresector,
										  centros."Estado" AS estado_area,
										  shp."REP_SUBPESCA2.ADM_UOT.PULLINQUE4_T_ACUICULTURA.ESPECIES" AS especies,
										  shp."REP_SUBPESCA2.ADM_UOT.PULLINQUE4_T_ACUICULTURA.TOPONIMIO" AS toponimio,
										  shp."REP_SUBPESCA2.ADM_UOT.PULLINQUE4_T_ACUICULTURA.T_ESTADOTRAMITE" AS t_estadotramite,
										  shp."REP_SUBPESCA2.ADM_UOT.PULLINQUE4_T_ACUICULTURA.F_RESOLSSP" AS f_resolucion,
										  shp."REP_SUBPESCA2.ADM_UOT.PULLINQUE4_T_ACUICULTURA.T_GRUPOESPECIE" AS t_grupoespecie,
										  shp."REP_SUBPESCA2.ADM_UOT.PULLINQUE4_T_ACUICULTURA.C_TIPOPORCION" AS c_tipoporcion,
										  shp."REP_SUBPESCA2.ADM_UOT.PULLINQUE4_T_ACUICULTURA.SUPERFICIETOTAL" AS superficie
FROM entradas.concesiones_acuicultura AS shp
LEFT JOIN entradas.centros_psmb AS centros
ON shp."REP_SUBPESCA2.ADM_UOT.PULLINQUE4_T_ACUICULTURA.N_CODIGOCENTRO" = centros."Código Centro"
WHERE shp."REP_SUBPESCA2.ADM_UOT.PULLINQUE4_T_ACUICULTURA.REGION" = 'REGIÓN DE LOS LAGOS' AND
											(shp."REP_SUBPESCA2.ADM_UOT.PULLINQUE4_T_ACUICULTURA.T_GRUPOESPECIE" = 'MOLUSCOS' OR
											shp."REP_SUBPESCA2.ADM_UOT.PULLINQUE4_T_ACUICULTURA.T_GRUPOESPECIE" = 'ABALONES o EQUINODERMOS') AND
											 shp."REP_SUBPESCA2.ADM_UOT.PULLINQUE4_T_ACUICULTURA.T_ESTADOTRAMITE" = 'CONCESION OTORGADA');

/* Se añade columna que indica si el centro es PSMB o no*/
ALTER TABLE capas_estaticas.centros_acuicultura ADD COLUMN psmb varchar(5);

UPDATE capas_estaticas.centros_acuicultura SET psmb =
										CASE WHEN 
										(codigoarea is NULL) OR 
										(estado_area = 'Suspendida') OR
										(estado_area = 'Eliminada') OR
										(codigocentro = 0)
											THEN 'No'
											ELSE 'Si'
										END;


---------------------------------------
-- /* 1.2 Centros PSMB y no PSMB*/ ----
---------------------------------------

/* Se separan los centros PSMB y no PSMB en distintas capas */

DROP TABLE IF EXISTS capas_estaticas.centros_psmb;
DROP TABLE IF EXISTS capas_estaticas.centros_no_psmb;

CREATE TABLE capas_estaticas.centros_psmb AS
(SELECT * FROM capas_estaticas.centros_acuicultura 
WHERE psmb = 'Si');

CREATE TABLE capas_estaticas.centros_no_psmb AS
(SELECT * FROM capas_estaticas.centros_acuicultura 
WHERE psmb = 'No');

----------------------------
-- /* 1.3 Áreas PSMB */ ----
----------------------------

/* Se crea una capa espacial con un polígono que envuelve a las áreas PSMB*/

DROP TABLE IF EXISTS capas_estaticas.areas_psmb;

CREATE TABLE capas_estaticas.areas_psmb AS
(SELECT centros.codigoarea, 
        ST_ConvexHull(ST_Collect(centros.geom)) as geom,
        areas."Nombre Área" as nombrearea,
        areas."Delimitación" as delim, 
        COUNT(DISTINCT centros.codigocentro) as n_centros,
        centros.estado_area, 
        areas."Fecha Estado Área" as fecha_est
FROM capas_estaticas.centros_psmb as centros
INNER JOIN entradas.areas_psmb as areas
ON centros.codigoarea = areas."Código Área"
WHERE centros.estado_area <> 'Eliminada'
GROUP BY centros.codigoarea, centros.estado_area, areas."Nombre Área", areas."Delimitación", areas."Fecha Estado Área");

---------------------------------------
-- /* 1.4 Existencias Moluscos */ ---
---------------------------------------

----------------------------------------
---- 1.4.1 Exitencias en centros -------
----------------------------------------

/* Vista que contiene el último mes registrado para cada centro */

CREATE TEMP TABLE ult_fecha AS
(SELECT "codigoCentro",
       MAX("periodoInformado") AS fecha
FROM entradas.existencias_moluscos
WHERE "periodoInformado" > (select MAX("periodoInformado") from entradas.existencias_moluscos) - interval '3 months'
GROUP BY "codigoCentro");

/* Se genera una tabla con la información de existencias en los ditntos centro de cultivo de moluscos */

DROP TABLE IF EXISTS capas_estaticas.centros_tara;

CREATE TABLE capas_estaticas.centros_tara AS
(SELECT centros.*, fechas.fecha, fechas.ton, fechas.fecha_3m
FROM (
SELECT ult_fecha."codigoCentro" as codigocentro,
		ult_fecha.fecha as fecha,
		existencias."Ton" as ton,
		existencias."Exist_3m" as fecha_3m
FROM ult_fecha
LEFT JOIN entradas.existencias_moluscos as existencias
ON ult_fecha."codigoCentro" = existencias."codigoCentro" and ult_fecha.fecha = existencias."periodoInformado"
) as fechas
RIGHT JOIN capas_estaticas.centros_acuicultura as centros
ON fechas.codigocentro = centros.codigocentro);

------------------------------------------------
------ /* 1.4.2 Existencias en Áreas */ --------
------------------------------------------------

DROP TABLE IF EXISTS capas_estaticas.areas_tara;

CREATE TABLE capas_estaticas.areas_tara AS
(SELECT centros.codigoarea,
	   ST_ConvexHull(ST_Collect(centros.geom)) as geom,
	   areas."Nombre Área" as nombre,
 	   SUM(DISTINCT centros.ton::float) as ton_total,
	   MAX(centros.fecha) as fecha_MAX
FROM capas_estaticas.centros_tara as centros
LEFT JOIN entradas.areas_psmb as areas
ON centros.codigoarea = areas."Código Área"
WHERE centros.codigoarea IS NOT NULL
GROUP BY centros.codigoarea, areas."Nombre Área");

---------------------------------------
-- /* 1.5 Existencias Salmónidos */ ---
---------------------------------------

-------------------------------------
----- /* 2. TABLAS MRSAT */ ---------
-------------------------------------

----------------------------------------------------
-- /* 2.1 Áreas según valores toxicológicos --------
----------------------------------------------------

/* Se une la información de grupo de toxinas y sus respectivos límites */

CREATE TEMP TABLE ult_pos AS(
	SELECT tox_grup.*, lim.tipo, lim.nm_toxina, lim.lim_cont, lim.lim_tox 
	FROM (SELECT gestio_sp.*, grupos.grupo
		  FROM entradas.gestio_sp
		  RIGHT JOIN entradas.grupos_toxinas grupos
		  ON gestio_sp."Análisis" = grupos.analisis) tox_grup
	LEFT JOIN entradas.limites_toxicologicos lim
	ON lim.grupo = tox_grup.grupo);

/* Se genera un ID único para cada estación, estación - grupo de toxinas y estación - análisis */

-- Se añanden las columnas que almacenan los nuevos id's

ALTER TABLE ult_pos
ADD COLUMN cod_estacion varchar(50),
ADD COLUMN cod_estacion_grupo varchar(70),
ADD COLUMN cod_estacion_analisis varchar(100);

-- Se concatenan los 'códigos'

UPDATE ult_pos
SET cod_estacion = "Còd. Centro Cultivo" || '-' || "Estación Monitoreo",
	cod_estacion_grupo = "Còd. Centro Cultivo" || '-' || "Estación Monitoreo" || '-' || grupo,
	cod_estacion_analisis = "Còd. Centro Cultivo" || '-' || "Estación Monitoreo" || '-' || "Análisis";
	
/* Se suman los análisis específicos para cada estación, según el grupo al que pertenezcan */

-- Se deja únicamente el último registro para cada toxina perteneciente a cada estación

CREATE TEMP TABLE ult_tox AS
(SELECT fech_tox.*, ult_pos."Resultado" as resultado_toxina
FROM (SELECT MAX("Fecha Extracción") AS fechaext,
								  cod_estacion,
								  cod_estacion_grupo,
								  cod_estacion_analisis,
								  "Estación Monitoreo" as estacion,
								  grupo,
								  "Análisis" as analisis,
								  "Código Área" as cod_area,
								  "Nombre Área" as n_area,
								  "Còd. Centro Cultivo" cod_centro,
								  tipo, nm_toxina, lim_cont, lim_tox 
FROM ult_pos
GROUP BY cod_estacion, cod_estacion_grupo, cod_estacion_analisis, "Estación Monitoreo", grupo, "Análisis",
		 "Código Área", "Nombre Área", "Còd. Centro Cultivo", tipo, nm_toxina, lim_cont, lim_tox) as fech_tox
LEFT JOIN ult_pos
ON ult_pos.cod_estacion_analisis = fech_tox.cod_estacion_analisis AND fech_tox.fechaext = ult_pos."Fecha Extracción"); 

-- Se suman los resultados de las toxinas pertenecientes a un mismo grupo para cada estación.
-- Además, se genera una columna con la acción asignada para cada toxina, de acuerdo a los límites preestablecidos

CREATE TEMP TABLE tox_est AS
(SELECT *, CASE WHEN resultado > lim_tox THEN 3
     WHEN (resultado > lim_cont AND resultado < lim_tox) THEN 2
	 ELSE 1 END AS n_accion
FROM (SELECT MAX(fechaext) as fechaext, cod_estacion,  cod_estacion_grupo,
	   estacion, grupo, cod_area, n_area, cod_centro, tipo, nm_toxina,
	   lim_cont,lim_tox, SUM(resultado_toxina) as resultado
FROM ult_tox
GROUP BY cod_estacion, cod_estacion_grupo, estacion, grupo, cod_area,
	   n_area, cod_centro, tipo, nm_toxina, lim_cont, lim_tox) as tox_est);

-- Se genera tabla para añadir la causal de la acción a cada área 

-- Se genera columna que almacena la diferencia entre el resultado de cada análisis y el máximo limite toxicológico

ALTER TABLE tox_est
ADD COLUMN dif_tox float;

UPDATE tox_est
SET dif_tox = resultado - lim_tox;

-- Para cada área se selecciona únicamente aquel registro que más se acerque al límite tóxico de cada toxina.

CREATE TEMP TABLE causal_area AS
(SELECT MAX_tox.*, CASE WHEN (dif_tox > 0) THEN grupo
					   WHEN (dif_tox < 0 AND resultado > lim_cont) THEN grupo
					   ELSE ' ' END AS causal
FROM (SELECT DISTINCT ON (cod_area)
cod_area, grupo, cod_centro, lim_cont, lim_tox, resultado, dif_tox
FROM tox_est
WHERE n_accion > 1
ORDER BY cod_area, dif_tox DESC) as MAX_tox);

-- Se 'pivotean' los resultados para cada toxina y se agrupar por estación

CREATE TEMP TABLE pivot_est AS
(SELECT cod_estacion, estacion, cod_area, cod_centro,
	   SUM(res_vpm) as res_vpm,
 	   MAX(fecha_vpm) as fecha_vpm,
	   SUM(res_vam) as res_vam,
 	   MAX(fecha_vam) as fecha_vam,
	   SUM(res_ytx) as res_ytx,
 	   MAX(fecha_ytx) as fecha_ytx,
	   SUM(res_ptx_ao) as res_ptx_ao,
 	   MAX(fecha_ptxao) as fecha_ptxao,
	   SUM(res_dtx) as res_dtx,
 	   MAX(fecha_dtx) as fecha_dtx,
	   SUM(res_aza) as res_aza,
 	   MAX(fecha_aza) as fecha_aza,
	   MAX(n_accion) as n_accion
FROM (SELECT *,
		  CASE WHEN (grupo = 'VPM') THEN resultado
		  ELSE 0 END AS res_vpm,
	  	  CASE WHEN (grupo = 'VPM') THEN tox_est.fechaext END AS fecha_vpm,
		  CASE WHEN (grupo = 'VAM') THEN resultado
		  ELSE 0 END AS res_vam,
	  	  CASE WHEN (grupo = 'VAM') THEN tox_est.fechaext END AS fecha_vam,
		  CASE WHEN (grupo = 'YTX') THEN resultado
		  ELSE 0 END AS res_ytx,
	  	  CASE WHEN (grupo = 'YTX') THEN tox_est.fechaext END AS fecha_ytx,
		  CASE WHEN (grupo = 'PTX_AO') THEN resultado
		  ELSE 0 END AS res_ptx_ao,
	  	  CASE WHEN (grupo = 'PTX_AO') THEN tox_est.fechaext END AS fecha_ptxao,
		  CASE WHEN (grupo = 'DTX') THEN resultado
		  ELSE 0 END AS res_dtx,
	  	  CASE WHEN (grupo = 'DTX') THEN tox_est.fechaext END AS fecha_dtx,
		  CASE WHEN (grupo = 'AZA') THEN resultado 
          ELSE 0 END AS res_aza,
	  	  CASE WHEN (grupo = 'AZA') THEN tox_est.fechaext END AS fecha_aza
	FROM tox_est) as pivot
GROUP BY cod_estacion, estacion, cod_area, cod_centro);

-- Se genera tabla de áreas
-- Se agrupan los resultados por área, se une la causal a cada área y se espacializan 

DROP TABLE IF EXISTS capas_estaticas.areas_contingencia;

CREATE TABLE capas_estaticas.areas_contingencia AS
(SELECT shp.geom, shp.codigoarea, shp.nombrearea, areas.*, shp.n_centros, shp.estado_area, shp.fecha_est 
FROM (SELECT piv.cod_area,
	   MAX(res_vpm) as res_vpm,
	   MAX(fecha_vpm) as fecha_vpm,
	   MAX(res_vam) as res_vam,
	   MAX(fecha_vam) as fecha_vam,
	   MAX(res_ytx) as res_ytx,
	   MAX(fecha_ytx) as fecha_ytx,
	   MAX(res_ptx_ao) as res_ptx_ao,
	   MAX(fecha_ptxao) as fecha_ptxao,
	   MAX(res_dtx) as res_dtx,
	   MAX(fecha_dtx) as fecha_dtx,
	   MAX(res_vpm) as res_aza,
	   MAX(fecha_vpm) as fecha_aza,
	   MAX(n_accion) as n_accion,
	   causal.causal
FROM pivot_est as piv
LEFT JOIN causal_area as causal
ON piv.cod_area = causal.cod_area
GROUP BY piv.cod_area, causal.causal) as areas
RIGHT JOIN capas_estaticas.areas_psmb as shp
ON shp.codigoarea = areas.cod_area);

-- Se generan campos para determinar la acción y un msje

ALTER TABLE capas_estaticas.areas_contingencia
ADD COLUMN accion varchar(80),
ADD COLUMN msje varchar(100); 

UPDATE capas_estaticas.areas_contingencia
SET accion = CASE WHEN n_accion = 3 THEN 'Cerrada'
				  WHEN n_accion = 2 THEN 'Abierta con monitoreo intensivo'
				  ELSE 'Abierta con monitoreo normal' END,
	msje = CASE WHEN n_accion = 3 THEN 'debido a la abundante presencia de'
				WHEN n_accion = 2 THEN 'debido a la presencia de'
				ELSE ', no hay presencia de toxinas' END;

-- Se eliminan las columnas que no sirven

ALTER TABLE capas_estaticas.areas_contingencia
DROP COLUMN cod_area, 
DROP COLUMN n_accion;

-- Se generan índices espaciales para las capas espaciales de salida

CREATE INDEX IF NOT EXISTS geom_id ON capas_estaticas.areas_contingencia USING GIST(geom);
CREATE INDEX IF NOT EXISTS geom_id ON capas_estaticas.areas_psmb USING GIST(geom);
CREATE INDEX IF NOT EXISTS geom_id ON capas_estaticas.areas_tara USING GIST(geom);
CREATE INDEX IF NOT EXISTS geom_id ON capas_estaticas.centros_acuicultura USING GIST(geom);
CREATE INDEX IF NOT EXISTS geom_id ON capas_estaticas.centros_no_psmb USING GIST(geom);
CREATE INDEX IF NOT EXISTS geom_id ON capas_estaticas.centros_psmb USING GIST(geom);
CREATE INDEX IF NOT EXISTS geom_id ON capas_estaticas.centros_tara USING GIST(geom);

