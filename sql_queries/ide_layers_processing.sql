-------------------------------------------------------
-- /* PROCESAMIENTO CAPAS ESPACIALES IDE SUBPESCA */ --
-------------------------------------------------------

-------------------------------
-- /* 1. Areas de Colecta */ --
-------------------------------
DROP TABLE IF EXISTS capas_estaticas.areas_colecta;

CREATE TABLE capas_estaticas.areas_colecta AS
(SELECT "REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_PO_AREACOLECTA.OBJECTID" objectid,
 		 geom,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.NOMBRE" nom_area,
		CASE 
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = -99) THEN 'SIN INFORMACION'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 0) THEN 'ZONA SIN DEMARCAR'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10101) THEN 'PUERTO MONTT'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10102) THEN 'CALBUCO'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10103) THEN 'COCHAMÓ'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10104) THEN 'FRESIA'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10105) THEN 'FRUTILLAR'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10106) THEN 'LOS MUERMOS'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10107) THEN 'LLANQUIHUE'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10108) THEN 'MAULLÍN'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10109) THEN 'PUERTO VARAS'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10201) THEN 'CASTRO'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10202) THEN 'ANCUD'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10203) THEN 'CHONCHI'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10204) THEN 'CURACO DE VÉLEZ'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10205) THEN 'DALCAHUE'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10206) THEN 'PUQUELDÓN'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10207) THEN 'QUEILÉN'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10208) THEN 'QUELLÓN'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10209) THEN 'QUEMCHI'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10210) THEN 'QUINCHAO'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10301) THEN 'OSORNO'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10302) THEN 'PUERTO OCTAY'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10303) THEN 'PURRANQUE'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10304) THEN 'PUYEHUE'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10305) THEN 'RÍO NEGRO'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10306) THEN 'SAN JUAN DE LA COSTA'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10307) THEN 'SAN PABLO'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10401) THEN 'CHAITÉN'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10402) THEN 'FUTALEUFÚ'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10403) THEN 'HUALAIHUÉ'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_COMUNA" = 10404) THEN 'PALENA'
 			ELSE 'SIN INFORMACIÓN'
 			END AS comuna,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.SECTOR" ubicacion,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.SUPERFICIE" superficie,
		CASE 
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_ESTADOAC" = 9) THEN 'DECLARADA ADMISIBLE'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.C_ESTADOAC" = 10) THEN 'DESTINACIÓN MARÍTIMA OTORGADA'
 			ELSE 'SIN INFORMACIÓN'
 			END AS estado,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.N_DECSSFFAA" n_decreto,
		timezone('posix/America/Santiago',to_timestamp("REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.F_DECSSFFAA"/1000)) f_decreto
FROM entradas.areas_colecta
WHERE "REP_SUBPESCA2.ADM_UOT.ACUI_SSPA_T_AREACOLECTA.COD_REGION" = 10);



--------------------
-- /* 2. ECMPO */ --
--------------------

DROP TABLE IF EXISTS capas_estaticas.ecmpo;

CREATE TABLE capas_estaticas.ecmpo AS
(SELECT "REP_SUBPESCA2.ADM_UAI.AIND_SSP_T_ECMPO.NOMBRE" nombre,
		geom,
		"REP_SUBPESCA2.ADM_UAI.AIND_SSP_T_ECMPO.SOLICITANTE" solicitante,
		"REP_SUBPESCA2.ADM_UAI.AIND_SSP_T_ECMPO.NATURALEZASECTOR" naturaleza,
		"REP_SUBPESCA2.ADM_UAI.AIND_SSP_T_ECMPO.C_ESTADOECMPO" estado,
		"REP_SUBPESCA2.ADM_UAI.AIND_SSP_T_ECMPO.N_INGRESOCI" n_ingreso,
		timezone('posix/America/Santiago',to_timestamp("REP_SUBPESCA2.ADM_UAI.AIND_SSP_T_ECMPO.F_INGRESOCI"/1000)) f_ingreso,
		timezone('posix/America/Santiago',to_timestamp("REP_SUBPESCA2.ADM_UAI.AIND_SSP_T_ECMPO.F_APRUEBAPLANADM"/1000)) f_aprob,
		"REP_SUBPESCA2.ADM_UAI.AIND_SSP_T_ECMPO.COMUNA" comuna,
		"REP_SUBPESCA2.ADM_UAI.AIND_SSP_PO_ECMPO.OBJECTID" objectid,
		"REP_SUBPESCA2.ADM_UAI.AIND_SSP_T_ECMPO.SUPERFICIE" superficie,
		"REP_SUBPESCA2.ADM_UAI.AIND_SSP_T_ECMPO.ES_ACTIVO" activo,
		"REP_SUBPESCA2.ADM_UAI.AIND_SSP_T_ECMPO.N_DECRETOMINDEF" n_decreto,
		timezone('posix/America/Santiago',to_timestamp("REP_SUBPESCA2.ADM_UAI.AIND_SSP_T_ECMPO.F_DECRETOMINDEF"/1000)) f_decreto
FROM entradas.ecmpo);


--------------------
-- /* 3. AMERB */ --
--------------------
DROP TABLE IF EXISTS capas_estaticas.amerb;

CREATE TABLE capas_estaticas.amerb AS 
(SELECT timezone('posix/America/Santiago',to_timestamp("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.F_ING_SSP"/1000)) fecha_ing,
		"REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.NOMBRE" nombre,
		geom,
		CASE 
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = -99) THEN 'SIN INFORMACION'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 0) THEN 'ZONA SIN DEMARCAR'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10101) THEN 'PUERTO MONTT'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10102) THEN 'CALBUCO'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10103) THEN 'COCHAMÓ'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10104) THEN 'FRESIA'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10105) THEN 'FRUTILLAR'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10106) THEN 'LOS MUERMOS'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10107) THEN 'LLANQUIHUE'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10108) THEN 'MAULLÍN'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10109) THEN 'PUERTO VARAS'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10201) THEN 'CASTRO'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10202) THEN 'ANCUD'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10203) THEN 'CHONCHI'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10204) THEN 'CURACO DE VÉLEZ'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10205) THEN 'DALCAHUE'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10206) THEN 'PUQUELDÓN'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10207) THEN 'QUEILÉN'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10208) THEN 'QUELLÓN'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10209) THEN 'QUEMCHI'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10210) THEN 'QUINCHAO'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10301) THEN 'OSORNO'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10302) THEN 'PUERTO OCTAY'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10303) THEN 'PURRANQUE'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10304) THEN 'PUYEHUE'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10305) THEN 'RÍO NEGRO'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10306) THEN 'SAN JUAN DE LA COSTA'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10307) THEN 'SAN PABLO'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10401) THEN 'CHAITÉN'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10402) THEN 'FUTALEUFÚ'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10403) THEN 'HUALAIHUÉ'
 			WHEN ("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.C_COMUNA" = 10404) THEN 'PALENA'
 			ELSE 'SIN INFORMACIÓN'
 			END AS comuna,
		"REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.DETALLEESTADO" d_estado,
		"REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.SUPERFICIE" superficie,
		"REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.N_MINECON" n_decreto_e,
		timezone('posix/America/Santiago',to_timestamp("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.F_MINECON"/1000)) f_decreto_e,
		"REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.N_DECRETOSSFFAA" n_decreto_d,
		timezone('posix/America/Santiago',to_timestamp("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.F_DECRETOSSFFAA"/1000)) f_decreto_d,
		"REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.N_DECRETORENOV" n_decreto_r,
		timezone('posix/America/Santiago',to_timestamp("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.F_DECRETORENOV"/1000)) f_decreto_r,
		"REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.VIGENCIA" vigencia,
		"REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.N_RPA" n_rpa,
		timezone('posix/America/Santiago',to_timestamp("REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.F_RPA"/1000)) f_rpa,
		"REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.ORGANIZACION" organizacion,
		"REP_SUBPESCA2.ADM_URB.RRBB_SSP_T_AMERB.ESPECIES" especies,
		"REP_SUBPESCA2.ADM_URB.RRBB_SSP_PO_AMERB.OBJECTID" objectid
FROM entradas.amerb);

-----------------------------------
-- /* 4. Acuicultura en AMERB */ --
-----------------------------------

DROP TABLE IF EXISTS capas_estaticas.acuiamerb;

CREATE TABLE capas_estaticas.acuiamerb AS
(SELECT "REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.TITULAR" titular,
		geom,
		CASE
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.C_TIPOACUIAME" = 0) THEN 'NO ESPECIFICADA'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.C_TIPOACUIAME" = 1) THEN 'AAMERB'
 			WHEN ("REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.C_TIPOACUIAME" = 2) THEN 'AEAMERB'
 			END AS tipo_actividad,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.N_PERT" n_pert,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.N_INGRESOCI" n_ingreso,
		timezone('posix/America/Santiago',to_timestamp("REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.F_INGRESOCI"/1000)) fecha_ing,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.CULTIVO" grupo_esp,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.ESPECIE" especies,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.SUPERFICIE" superficie,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.UBICACIONGEOG" ubicacion,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.Comuna" comuna,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.TIPOCONCESION" t_concesion,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.ESTADO" est_tram,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSP_PO_ACUICULTURA.OBJECTID" objectid,
		"REP_SUBPESCA2.ADM_UOT.ACUI_SSP_T_ACUICULTURAAMERB.TipoAmerb" tipo
FROM entradas.acuiamerb);
