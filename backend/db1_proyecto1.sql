--CARGA MODELO
CREATE TABLE "SYSTEM"."HOSPITAL" 
   (	"ID_HOSPITAL" NUMBER DEFAULT "SYSTEM"."SECUENCIA_ID"."NEXTVAL", 
	"NOMBRE_HOSPITAL" VARCHAR2(50 BYTE), 
	"DIRECCION_HOSPITAL" VARCHAR2(200 BYTE), 
	 PRIMARY KEY ("ID_HOSPITAL")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM"  ENABLE, 
	 CONSTRAINT "FK_HOSPITAL_VIC" FOREIGN KEY ("ID_HOSPITAL")
	  REFERENCES "SYSTEM"."HOSPITAL" ("ID_HOSPITAL") DISABLE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM" ;
  

CREATE TABLE "SYSTEM"."VICTIMA" 
   (	"ID_VICTIMA" NUMBER DEFAULT "SYSTEM"."SECUENCIA_ID"."NEXTVAL", 
	"NOMBRE_VICTIMA" VARCHAR2(50 BYTE) NOT NULL ENABLE, 
	"APELLIDO_VICTIMA" VARCHAR2(50 BYTE) NOT NULL ENABLE, 
	"DIRECCION_VICTIMA" VARCHAR2(200 BYTE) NOT NULL ENABLE, 
	"FECHA_CONFIRMACION" DATE NOT NULL ENABLE, 
	"FECHA_MUERTE" DATE, 
	"ESTADO_VICTIMA" VARCHAR2(200 BYTE) NOT NULL ENABLE, 
	"ID_HOSPITAL" NUMBER DEFAULT "SYSTEM"."SECUENCIA_ID"."NEXTVAL", 
	"FECHA_PRIMERA_SOSPECHA" DATE NOT NULL ENABLE, 
	 PRIMARY KEY ("ID_VICTIMA")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM"  ENABLE, 
	 CONSTRAINT "FK_HOSPITAL_VICTIMA" FOREIGN KEY ("ID_HOSPITAL")
	  REFERENCES "SYSTEM"."HOSPITAL" ("ID_HOSPITAL") DISABLE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM" ;
  
CREATE TABLE "SYSTEM"."UBICACION" 
   (	"ID_UBICACION" NUMBER DEFAULT "SYSTEM"."SECUENCIA_ID"."NEXTVAL", 
	"UBICACION_VICTIMA" VARCHAR2(200 BYTE), 
	"FECHA_LLEGADA" DATE, 
	"FECHA_RETIRO" DATE, 
	"ID_VICTIMA" NUMBER DEFAULT "SYSTEM"."SECUENCIA_ID"."NEXTVAL", 
	 PRIMARY KEY ("ID_UBICACION")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM"  ENABLE, 
	 CONSTRAINT "FK_VICTIMA_UBICACION" FOREIGN KEY ("ID_VICTIMA")
	  REFERENCES "SYSTEM"."VICTIMA" ("ID_VICTIMA") ENABLE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM" ;
  
  CREATE TABLE "SYSTEM"."TRATAMIENTO" 
   (	"TRATAMIENTO" VARCHAR2(200 BYTE), 
	"FECHA_INICIO_TRATAMIENTO" DATE, 
	"FECHA_FIN_TRATAMIENTO" DATE, 
	"EFECTIVIDAD" NUMBER, 
	"EFECTIVIDAD_EN_VICTIMA" NUMBER, 
	"ID_TRATAMIENTO" NUMBER DEFAULT "SYSTEM"."SECUENCIA_ID"."NEXTVAL", 
	"ID_VICTIMA" NUMBER DEFAULT "SYSTEM"."SECUENCIA_ID"."NEXTVAL", 
	 PRIMARY KEY ("ID_TRATAMIENTO")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM"  ENABLE, 
	 CONSTRAINT "FK_VICTIMA_TRATAMIENTO" FOREIGN KEY ("ID_VICTIMA")
	  REFERENCES "SYSTEM"."VICTIMA" ("ID_VICTIMA") ENABLE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM" ;
  
  
  CREATE TABLE "SYSTEM"."ASOCIADO" 
   (	"ID_ASOCIADO" NUMBER DEFAULT "SYSTEM"."SECUENCIA_ID"."NEXTVAL", 
	"NOMBRE_ASOCIADO" VARCHAR2(50 BYTE) NOT NULL ENABLE, 
	"APELLIDO_ASOCIADO" VARCHAR2(50 BYTE) NOT NULL ENABLE, 
	"FECHA_CONOCIO" DATE, 
	 PRIMARY KEY ("ID_ASOCIADO")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM"  ENABLE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM" ;
  
   CREATE TABLE "SYSTEM"."VICTIMA_ASOCIADO" 
   (	"ID_VICTIMA" NUMBER DEFAULT "SYSTEM"."SECUENCIA_ID"."NEXTVAL" NOT NULL ENABLE, 
	"ID_ASOCIADO" NUMBER DEFAULT "SYSTEM"."SECUENCIA_ID"."NEXTVAL" NOT NULL ENABLE, 
	"ID_VICTIMA_ASOCIADO" NUMBER DEFAULT "SYSTEM"."SECUENCIA_ID"."NEXTVAL", 
	"NOMBRE_VICTIMA" VARCHAR2(50 BYTE) NOT NULL ENABLE, 
	 PRIMARY KEY ("ID_VICTIMA_ASOCIADO")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM"  ENABLE, 
	 FOREIGN KEY ("ID_VICTIMA")
	  REFERENCES "SYSTEM"."VICTIMA" ("ID_VICTIMA") ENABLE, 
	 FOREIGN KEY ("ID_ASOCIADO")
	  REFERENCES "SYSTEM"."ASOCIADO" ("ID_ASOCIADO") ENABLE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM" ;


  CREATE TABLE "SYSTEM"."TIPO_CONTACTO" 
   (	"ID_CONTACTO" NUMBER DEFAULT "SYSTEM"."SECUENCIA_ID"."NEXTVAL", 
	"CONTACTO_FISICO" VARCHAR2(200 BYTE) NOT NULL ENABLE, 
	"FECHA_INICIO_CONTACTO" DATE NOT NULL ENABLE, 
	"FECHA_FIN_CONTACTO" DATE NOT NULL ENABLE, 
	"ID_VICTIMA_ASOCIADO" NUMBER DEFAULT "SYSTEM"."SECUENCIA_ID"."NEXTVAL" NOT NULL ENABLE, 
	 PRIMARY KEY ("ID_CONTACTO")
  USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS 
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM"  ENABLE
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM" ;



execute migrar;

DELETE FROM hospital
WHERE id_hospital NOT IN (
    SELECT MIN(id_hospital)
    FROM hospital
    GROUP BY  nombre_hospital, direccion_hospital
);


execute migrar_victima;

DELETE FROM victima
WHERE id_victima NOT IN (
    SELECT MIN(id_victima)
    FROM victima
    GROUP BY  NOMBRE_VICTIMA, APELLIDO_VICTIMA, DIRECCION_VICTIMA, FECHA_CONFIRMACION, FECHA_MUERTE, ESTADO_VICTIMA, FECHA_PRIMERA_SOSPECHA, id_hospital
);



execute migrar_ubicacion;

DELETE FROM ubicacion
WHERE id_ubicacion NOT IN (
    SELECT MIN(id_ubicacion)
    FROM ubicacion
    GROUP BY id_victima, ubicacion_victima, fecha_llegada, fecha_retiro
);



execute migrar_tratamiento;

DELETE FROM TRATAMIENTO
WHERE id_TRATAMIENTO NOT IN (
    SELECT MIN(id_TRATAMIENTO)
    FROM TRATAMIENTO
    GROUP BY id_victima, tratamiento, fecha_inicio_tratamiento, fecha_fin_tratamiento, efectividad_en_victima,efectividad
);




execute migrar_asociado;

DELETE FROM asociado
WHERE id_asociado NOT IN (
    SELECT MIN(id_asociado)
    FROM asociado
    GROUP BY nombre_asociado, apellido_asociado, fecha_conocio
);

INSERT INTO victima_asociado ( id_asociado, id_victima, nombre_victima)
SELECT a.id_asociado, v.id_victima , v.nombre_victima
FROM victima v
INNER JOIN tabla_temporal t ON v.nombre_victima = t.nombre_victima
INNER JOIN asociado a ON a.nombre_asociado = t.nombre_asociado AND a.apellido_asociado = t.apellido_asociado and t.fecha_conocio=a.fecha_conocio
WHERE v.id_victima IN (SELECT id_victima FROM victima);

DELETE FROM victima_asociado
WHERE id_victima_asociado NOT IN (
    SELECT MIN(id_victima_asociado)
    FROM victima_asociado
    GROUP BY id_asociado, id_victima
);


execute migrar_tipo_contacto;

DELETE FROM tipo_contacto
WHERE id_contacto NOT IN (
    SELECT MIN(id_contacto)
    FROM tipo_contacto
    GROUP BY id_victima_asociado, contacto_fisico, fecha_inicio_contacto, fecha_fin_contacto
);


-- ELIMINAR MODELO
drop table tipo_contacto;
drop table tratamiento;
drop table ubicacion;
drop table victima_asociado;
drop table asociado;
drop table victima;
drop table hospital;


--ELIMINAR TEMPORAL
DROP TABLE TABLA_TEMPORAL;

--CARGAR TEMPORAL
  CREATE TABLE "SYSTEM"."TABLA_TEMPORAL" 
   (	"NOMBRE_VICTIMA" VARCHAR2(50 BYTE), 
	"APELLIDO_VICTIMA" VARCHAR2(50 BYTE), 
	"DIRECCION_VICTIMA" VARCHAR2(200 BYTE), 
	"FECHA_PRIMERA_SOSPECHA" DATE, 
	"FECHA_CONFIRMACION" DATE, 
	"FECHA_MUERTE" DATE, 
	"ESTADO_VICTIMA" VARCHAR2(200 BYTE), 
	"NOMBRE_ASOCIADO" VARCHAR2(50 BYTE), 
	"APELLIDO_ASOCIADO" VARCHAR2(50 BYTE), 
	"FECHA_CONOCIO" DATE, 
	"CONTACTO_FISICO" VARCHAR2(200 BYTE), 
	"FECHA_INICIO_CONTACTO" DATE, 
	"FECHA_FIN_CONTACTO" DATE, 
	"NOMBRE_HOSPITAL" VARCHAR2(50 BYTE), 
	"DIRECCION_HOSPITAL" VARCHAR2(200 BYTE), 
	"UBICACION_VICTIMA" VARCHAR2(200 BYTE), 
	"FECHA_LLEGADA" DATE, 
	"FECHA_RETIRO" DATE, 
	"TRATAMIENTO" VARCHAR2(200 BYTE), 
	"EFECTIVIDAD" NUMBER, 
	"FECHA_INICIO_TRATAMIENTO" DATE, 
	"FECHA_FIN_TRATAMIENTO" DATE, 
	"EFECTIVIDAD_EN_VICTIMA" NUMBER
   ) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 
 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1
  BUFFER_POOL DEFAULT FLASH_CACHE DEFAULT CELL_FLASH_CACHE DEFAULT)
  TABLESPACE "SYSTEM" ;

CREATE TABLE victima_asociado (
    id_victima NUMBER DEFAULT secuencia_id.NEXTVAL,
    id_asociado NUMBER DEFAULT secuencia_id.NEXTVAL,
    FOREIGN KEY (id_victima) REFERENCES victima(id_victima),
    FOREIGN KEY (id_asociado) REFERENCES asociado(id_asociado)
);


ALTER TABLE tratamiento 
ADD CONSTRAINT pk_id_TRATAMIENTO PRIMARY KEY (id_TRATAMIENTO);

ALTER TABLE victima_asociado ADD id_victima_asociado NUMBER DEFAULT secuencia_id.NEXTVAL PRIMARY KEY;
ALTER TABLE TRATAMIENTO ADD id_VICTIMA NUMBER DEFAULT secuencia_id.NEXTVAL ;

ALTER TABLE tipo_contacto
ADD CONSTRAINT fk_contacto_victima_asociado FOREIGN KEY (id_VICTIMA_asociado) REFERENCES VICTIMA_asociado(id_VICTIMA_asociado);

ALTER TABLE tabla_hija
ADD CONSTRAINT fk_tabla_hija_tabla_padre FOREIGN KEY (id_padre) REFERENCES tabla_padre(id);

INSERT INTO Victima(NOMBRE_VICTIMA,APELLIDO_VICTIMA, DIRECCION_VICTIMA,FECHA_PRIMERA_SOSPECHA,FECHA_CONFIRMACION,FECHA_MUERTE,ESTADO_VICTIMA)
VALUES ('juan','de leon', 'manzana31',TO_DATE('11/02/2021  00:00:00', 'DD-MM-YYYY HH24:MI:SS'),TO_DATE('11/02/2021  00:00:00', 'DD-MM-YYYY HH24:MI:SS '),TO_DATE('11/02/2021  00:00:00', 'DD-MM-YYYY HH24:MI:SS'),'bien jej');

truncate table tabla_temporal
truncate table hospital
truncate table victima
truncate table asociado
truncate table tipo_contacto
truncate table tratamiento
truncate table ubicacion
truncate table victima_asociado

DROP SEQUENCE SECUENCIA_ID;
CREATE SEQUENCE  "SYSTEM"."SECUENCIA_ID"  MINVALUE 1 MAXVALUE 9999999999999999999999999999 INCREMENT BY 1 START WITH 2997461 CACHE 20 NOORDER  NOCYCLE  NOKEEP  NOSCALE  GLOBAL ;


ALTER TABLE TABLA_TEMPORAL ADD id_TABLA_TEMPORAL NUMBER DEFAULT secuencia_id.NEXTVAL;


MERGE INTO tabla_temporal T
USING hospital S
ON (T.nombre_hospital = S.nombre_hospital AND T.direccion_hospital = S.direccion_hospital)
WHEN MATCHED THEN
UPDATE SET T.nombre_hospital = S.nombre_hospital, T.direccion_hospital = S.direccion_hospital
WHEN NOT MATCHED THEN
INSERT (nombre_hospital, direccion_hospital) VALUES (S.nombre_hospital, S.direccion_hospital);



/*INSERT INTO HOSPITAL (ID_HOSPITAL, NOMBRE_HOSPITAL, DIRECCION_HOSPITAL)
SELECT ID_TABLA_TEMPORAL, NOMBRE_HOSPITAL, DIRECCION_HOSPITAL
FROM (
  SELECT ROW_NUMBER() OVER (PARTITION BY NOMBRE_HOSPITAL ORDER BY ID_TABLA_TEMPORAL) AS RN,
         ID_TABLA_TEMPORAL,
         NOMBRE_HOSPITAL,
         DIRECCION_HOSPITAL
  FROM TABLA_TEMPORAL
  WHERE NOMBRE_HOSPITAL IS NOT NULL
) T
WHERE RN = 1;*/
--HOSPITAL

CREATE OR REPLACE PROCEDURE migrar IS
  CURSOR c_origen IS
    SELECT nombre_hospital, direccion_hospital
    FROM tabla_temporal
    WHERE nombre_hospital IS NOT NULL AND direccion_hospital IS NOT NULL;
BEGIN
  FOR r IN c_origen LOOP
    INSERT INTO hospital(nombre_hospital, direccion_hospital)
    VALUES (r.nombre_hospital, r.direccion_hospital);
  END LOOP;
END;

execute migrar;

DELETE FROM hospital
WHERE id_hospital NOT IN (
    SELECT MIN(id_hospital)
    FROM hospital
    GROUP BY  nombre_hospital, direccion_hospital
);

--VICTIMA
CREATE OR REPLACE PROCEDURE migrar_victima IS
  CURSOR c_origen IS
    SELECT tt.NOMBRE_VICTIMA, tt.APELLIDO_VICTIMA, tt.DIRECCION_VICTIMA, tt.FECHA_CONFIRMACION, tt.FECHA_MUERTE, tt.ESTADO_VICTIMA, tt.FECHA_PRIMERA_SOSPECHA, h.id_hospital
    FROM tabla_temporal tt
    LEFT JOIN hospital h ON tt.nombre_hospital = h.nombre_hospital
    AND tt.direccion_hospital = h.direccion_hospital
    WHERE tt.NOMBRE_VICTIMA IS NOT NULL;
BEGIN
  FOR r IN c_origen LOOP
    INSERT INTO victima(NOMBRE_VICTIMA, APELLIDO_VICTIMA, DIRECCION_VICTIMA, FECHA_CONFIRMACION, FECHA_MUERTE, ESTADO_VICTIMA, FECHA_PRIMERA_SOSPECHA, id_hospital)
    VALUES (r.NOMBRE_VICTIMA, r.APELLIDO_VICTIMA, r.DIRECCION_VICTIMA, r.FECHA_CONFIRMACION, r.FECHA_MUERTE, r.ESTADO_VICTIMA, r.FECHA_PRIMERA_SOSPECHA, r.id_hospital);
  END LOOP;
END;

execute migrar_victima;

DELETE FROM victima
WHERE id_victima NOT IN (
    SELECT MIN(id_victima)
    FROM victima
    GROUP BY  NOMBRE_VICTIMA, APELLIDO_VICTIMA, DIRECCION_VICTIMA, FECHA_CONFIRMACION, FECHA_MUERTE, ESTADO_VICTIMA, FECHA_PRIMERA_SOSPECHA, id_hospital
);




/*INSERT INTO VICTIMA (ID_VICTIMA, ID_HOSPITAL, NOMBRE_VICTIMA, APELLIDO_VICTIMA, DIRECCION_VICTIMA, FECHA_CONFIRMACION, FECHA_MUERTE, ESTADO_VICTIMA, FECHA_PRIMERA_SOSPECHA)
SELECT ID_TABLA_TEMPORAL, HOSPITAL.ID_HOSPITAL, NOMBRE_VICTIMA, APELLIDO_VICTIMA, DIRECCION_VICTIMA, FECHA_CONFIRMACION, FECHA_MUERTE, ESTADO_VICTIMA, FECHA_PRIMERA_SOSPECHA
FROM (
  SELECT ROW_NUMBER() OVER (PARTITION BY NOMBRE_VICTIMA ORDER BY ID_TABLA_TEMPORAL) RN, ID_TABLA_TEMPORAL, NOMBRE_VICTIMA, APELLIDO_VICTIMA, DIRECCION_VICTIMA, FECHA_CONFIRMACION, FECHA_MUERTE, ESTADO_VICTIMA, FECHA_PRIMERA_SOSPECHA
  FROM TABLA_TEMPORAL
  WHERE NOMBRE_VICTIMA IS NOT NULL
) T
LEFT JOIN HOSPITAL ON T.ID_TABLA_TEMPORAL = HOSPITAL.ID_HOSPITAL
WHERE RN = 1;*/




--UBICACION

CREATE OR REPLACE PROCEDURE migrar_ubicacion IS
  CURSOR c_origen IS
    SELECT  tt.ubicacion_victima, tt.fecha_llegada, tt.fecha_retiro, v.id_victima
    FROM tabla_temporal tt
    LEFT JOIN victima v ON tt.nombre_victima = v.nombre_victima
    AND tt.direccion_victima = v.direccion_victima
    AND tt.apellido_victima = v.apellido_victima
    WHERE tt.ubicacion_victima IS NOT NULL;
BEGIN
  FOR r IN c_origen LOOP
    INSERT INTO ubicacion(ubicacion_victima, fecha_llegada, fecha_retiro, id_victima)
    VALUES (r.ubicacion_victima, r.fecha_llegada, r.fecha_retiro, r.id_victima);
  END LOOP;
END;

execute migrar_ubicacion;

DELETE FROM ubicacion
WHERE id_ubicacion NOT IN (
    SELECT MIN(id_ubicacion)
    FROM ubicacion
    GROUP BY id_victima, ubicacion_victima, fecha_llegada, fecha_retiro
);

/*INSERT INTO ubicacion (id_victima, ubicacion_victima, fecha_llegada, fecha_retiro)
SELECT v.id_victima, t.ubicacion_victima, t.fecha_llegada, t.fecha_retiro
FROM victima v
INNER JOIN tabla_temporal t ON v.nombre_victima = t.nombre_victima
WHERE t.ubicacion_victima IS NOT NULL AND v.id_victima IN (SELECT id_victima FROM victima);

SELECT id_victima, ubicacion_victima, fecha_llegada, fecha_retiro, COUNT(*) as cantidad
FROM ubicacion
GROUP BY id_victima, ubicacion_victima, fecha_llegada, fecha_retiro
HAVING COUNT(*) > 1;*/



--TRATAMIENTO
CREATE OR REPLACE PROCEDURE migrar_tratamiento IS
  CURSOR c_origen IS
    SELECT tt.tratamiento, tt.fecha_inicio_tratamiento, tt.fecha_fin_tratamiento, tt.efectividad_en_victima, tt.efectividad, v.id_victima
    FROM tabla_temporal tt
    LEFT JOIN victima v ON tt.nombre_victima = v.nombre_victima
    AND tt.direccion_victima = v.direccion_victima
    AND tt.apellido_victima = v.apellido_victima
    WHERE tt.tratamiento IS NOT NULL;
BEGIN
  FOR r IN c_origen LOOP
    INSERT INTO tratamiento(tratamiento, fecha_inicio_tratamiento, fecha_fin_tratamiento, efectividad_en_victima, efectividad, id_victima)
    VALUES (r.tratamiento, r.fecha_inicio_tratamiento, r.fecha_fin_tratamiento, r.efectividad_en_victima, r.efectividad, r.id_victima);
  END LOOP;
END;

execute migrar_tratamiento;

DELETE FROM TRATAMIENTO
WHERE id_TRATAMIENTO NOT IN (
    SELECT MIN(id_TRATAMIENTO)
    FROM TRATAMIENTO
    GROUP BY id_victima, tratamiento, fecha_inicio_tratamiento, fecha_fin_tratamiento, efectividad_en_victima,efectividad
);

/*INSERT INTO tratamiento (id_victima, tratamiento, fecha_inicio_tratamiento, fecha_fin_tratamiento, efectividad_en_victima,efectividad)
SELECT v.id_victima, t.tratamiento, t.fecha_inicio_tratamiento, t.fecha_fin_tratamiento, t.efectividad_en_victima,efectividad
FROM victima v
INNER JOIN tabla_temporal t ON v.nombre_victima = t.nombre_victima
WHERE t.tratamiento IS NOT NULL AND v.id_victima IN (SELECT id_victima FROM victima);

SELECT id_victima, tratamiento, fecha_inicio_tratamiento, fecha_fin_tratamiento, efectividad_en_victima,efectividad, COUNT(*) as cantidad
FROM TRATAMIENTO
GROUP BY id_victima, tratamiento, fecha_inicio_tratamiento, fecha_fin_tratamiento, efectividad_en_victima,efectividad
HAVING COUNT(*) > 1;*/


--ASOCIADO

CREATE OR REPLACE PROCEDURE migrar_asociado IS
  CURSOR c_origen IS
    SELECT nombre_asociado, apellido_asociado, fecha_conocio
    FROM tabla_temporal
    WHERE nombre_asociado IS NOT NULL AND apellido_asociado IS NOT NULL;
BEGIN
  FOR r IN c_origen LOOP
    INSERT INTO asociado(nombre_asociado, apellido_asociado, fecha_conocio)
    VALUES (r.nombre_asociado, r.apellido_asociado, r.fecha_conocio);
  END LOOP;
END;

execute migrar_asociado;

DELETE FROM asociado
WHERE id_asociado NOT IN (
    SELECT MIN(id_asociado)
    FROM asociado
    GROUP BY nombre_asociado, apellido_asociado, fecha_conocio
);

/*INSERT INTO asociado (id_asociado, nombre_asociado, apellido_asociado, fecha_conocio)
SELECT id_tabla_temporal, nombre_asociado, apellido_asociado, fecha_conocio
FROM tabla_temporal
WHERE nombre_asociado IS NOT NULL;

INSERT INTO asociado (  nombre_asociado, apellido_asociado, fecha_conocio)
SELECT  t.nombre_asociado, t.apellido_asociado, t.fecha_conocio
FROM victima v
INNER JOIN tabla_temporal t ON v.nombre_victima = t.nombre_victima
WHERE t.nombre_asociado IS NOT NULL AND v.id_victima IN (SELECT id_victima FROM victima);

SELECT nombre_asociado, apellido_asociado, fecha_conocio, COUNT(*) as cantidad
FROM asociado
GROUP BY nombre_asociado, apellido_asociado, fecha_conocio
HAVING COUNT(*) > 1;*/

--VICTIMA_ASOCIADO



INSERT INTO victima_asociado (id_victima, id_asociado)
SELECT  v.id_victima, a.id_asociado
FROM victima v
INNER JOIN tabla_temporal t ON v.nombre_victima = t.nombre_victima
INNER JOIN asociado a ON a.nombre_asociado = t.nombre_asociado
WHERE v.id_victima IS NOT NULL AND a.id_asociado IS NOT NULL;

INSERT INTO victima_asociado ( id_asociado, id_victima, nombre_victima)
SELECT a.id_asociado, v.id_victima , v.nombre_victima
FROM victima v
INNER JOIN tabla_temporal t ON v.nombre_victima = t.nombre_victima
INNER JOIN asociado a ON a.nombre_asociado = t.nombre_asociado AND a.apellido_asociado = t.apellido_asociado and t.fecha_conocio=a.fecha_conocio
WHERE v.id_victima IN (SELECT id_victima FROM victima);

DELETE FROM victima_asociado
WHERE id_victima_asociado NOT IN (
    SELECT MIN(id_victima_asociado)
    FROM victima_asociado
    GROUP BY id_asociado, id_victima
);

SELECT id_asociado, id_victima, COUNT(*) as cantidad
FROM victima_asociado
GROUP BY id_asociado, id_victima
HAVING COUNT(*) > 1;



--TIPO_CONTACTO
CREATE OR REPLACE PROCEDURE migrar_tipo_contacto IS
  CURSOR c_origen IS
    SELECT tt.contacto_fisico, tt.fecha_inicio_contacto, tt.fecha_fin_contacto, v.id_victima_asociado
    FROM tabla_temporal tt
    LEFT JOIN victima_asociado v ON tt.nombre_victima = v.nombre_victima
    WHERE tt.contacto_fisico IS NOT NULL;
BEGIN
  FOR r IN c_origen LOOP
    INSERT INTO tipo_contacto(contacto_fisico, fecha_inicio_contacto, fecha_fin_contacto, id_victima_asociado)
    VALUES (r.contacto_fisico, r.fecha_inicio_contacto, r.fecha_fin_contacto, r.id_victima_asociado);
  END LOOP;
END;

execute migrar_tipo_contacto;

DELETE FROM tipo_contacto
WHERE id_contacto NOT IN (
    SELECT MIN(id_contacto)
    FROM tipo_contacto
    GROUP BY id_victima_asociado, contacto_fisico, fecha_inicio_contacto, fecha_fin_contacto
);
/*
INSERT INTO tipo_contacto ( id_victima_asociado, contacto_fisico, fecha_inicio_contacto, fecha_fin_contacto)
SELECT va.id_victima, t.contacto_fisico, t.fecha_inicio_contacto, t.fecha_fin_contacto
FROM tabla_temporal t
INNER JOIN victima va ON va.id_victima = t.id_tabla_temporal
WHERE t.contacto_fisico IS NOT NULL;

SELECT id_victima_asociado, contacto_fisico, fecha_inicio_contacto, fecha_fin_contacto, COUNT(*) as cantidad
FROM tipo_contacto
GROUP BY id_victima_asociado, contacto_fisico, fecha_inicio_contacto, fecha_fin_contacto
HAVING COUNT(*) > 1;*/





--PRIMERA CONSULTA
SELECT h.nombre_hospital, h.direccion_hospital, COUNT(v.id_victima) AS fallecidos
FROM hospital h
LEFT JOIN victima v ON h.id_hospital = v.id_hospital
WHERE v.fecha_muerte IS NOT NULL
GROUP BY h.nombre_hospital, h.direccion_hospital;


--SEGUNDA CONSULTA
SELECT v.nombre_victima, v.apellido_victima, t.tratamiento
FROM VICTIMA v
JOIN TRATAMIENTO t ON v.id_victima = t.id_victima
WHERE t.tratamiento = 'Transfusiones de sangre'
AND t.efectividad_en_victima > 5;


--TERCERA CONSULTA
SELECT v.nombre_victima, v.apellido_victima, v.direccion_victima, COUNT(va.id_asociado) AS num_asociados
FROM victima v
JOIN victima_asociado va ON v.id_victima = va.id_victima
JOIN asociado a ON va.id_asociado = a.id_asociado
WHERE v.fecha_muerte IS NOT NULL
GROUP BY v.nombre_victima, v.apellido_victima, v.direccion_victima
HAVING COUNT(va.id_asociado) > 3;

--CUARTA CONSULTA
SELECT v.nombre_victima, v.apellido_victima
FROM victima v
JOIN victima_asociado va ON v.id_victima = va.id_victima
JOIN tipo_contacto tc ON va.id_victima_asociado = tc.id_victima_asociado
WHERE v.estado_victima = 'Suspendida' AND tc.contacto_fisico = 'Beso'
GROUP BY v.nombre_victima, v.apellido_victima
HAVING COUNT(DISTINCT va.id_asociado) > 2;

--QUINTA CONSULTA
SELECT v.nombre_victima, COUNT(*) as cantidad_tratamientos
FROM victima v
JOIN tratamiento t ON v.id_victima = t.id_victima
WHERE t.tratamiento = 'Oxigeno'
GROUP BY v.nombre_victima
ORDER BY cantidad_tratamientos DESC
FETCH FIRST 5 ROWS ONLY;

--SEXTA CONSULTA
SELECT v.nombre_victima, v.apellido_victima, v.fecha_muerte 
FROM victima v 
JOIN tratamiento t ON v.id_victima = t.id_victima 
JOIN ubicacion u ON v.id_victima = u.id_victima 
WHERE u.ubicacion_victima = '1987 Delphine Well' 
AND t.tratamiento = 'Manejo de la presion arterial'
AND v.fecha_muerte IS NOT NULL;

--SEPTIMA CONSULTA
SELECT v.nombre_victima, v.apellido_victima, v.direccion_victima 
FROM Victima v
JOIN Victima_Asociado va ON v.id_victima = va.id_victima
JOIN Asociado a ON va.id_asociado = a.id_asociado
JOIN Hospital h ON v.id_hospital = h.id_hospital
JOIN Tratamiento t ON v.id_victima = t.id_victima
GROUP BY v.nombre_victima, v.apellido_victima, v.direccion_victima
HAVING COUNT(DISTINCT va.id_asociado) < 2 AND COUNT(DISTINCT t.id_tratamiento) = 2;

--OCTAVA CONSULTA
SELECT 
    TO_CHAR(fecha_primera_sospecha, 'MM') AS mes, 
    nombre_victima, 
    apellido_victima, 
    COUNT(*) AS total_tratamientos
FROM 
    VICTIMA 
    JOIN TRATAMIENTO ON VICTIMA.id_victima = TRATAMIENTO.id_victima
GROUP BY 
    TO_CHAR(fecha_primera_sospecha, 'MM'),
    nombre_victima, 
    apellido_victima
HAVING 
    COUNT(*) = (
        SELECT 
            MAX(total_tratamientos)
        FROM 
            (
                SELECT 
                    VICTIMA.nombre_victima, 
                    VICTIMA.apellido_victima, 
                    COUNT(*) AS total_tratamientos
                FROM 
                    VICTIMA 
                    JOIN TRATAMIENTO ON VICTIMA.id_victima = TRATAMIENTO.id_victima
                GROUP BY 
                    VICTIMA.nombre_victima, 
                    VICTIMA.apellido_victima
            )
    ) 
    OR 
    COUNT(*) = (
        SELECT 
            MIN(total_tratamientos)
        FROM 
            (
                SELECT 
                    VICTIMA.nombre_victima, 
                    VICTIMA.apellido_victima, 
                    COUNT(*) AS total_tratamientos
                FROM 
                    VICTIMA 
                    JOIN TRATAMIENTO ON VICTIMA.id_victima = TRATAMIENTO.id_victima
                GROUP BY 
                    VICTIMA.nombre_victima, 
                    VICTIMA.apellido_victima
            )
    )
ORDER BY 
    mes ASC,
    total_tratamientos DESC;


--NOVENA CONSULTA

SELECT h.nombre_hospital, 
       TO_CHAR(COUNT(v.id_victima) / SUM(COUNT(v.id_victima)) OVER() * 100, 'FM999.00') AS porcentaje_victimas
FROM hospital h
LEFT JOIN victima v ON h.id_hospital = v.id_hospital
GROUP BY h.nombre_hospital
ORDER BY porcentaje_victimas DESC;

--DECIMA CONSULTA
SELECT h.nombre_hospital, t.contacto_fisico, 
  COUNT(*) / SUM(COUNT(*)) OVER(PARTITION BY h.id_hospital) * 100 AS porcentaje_victimas
FROM hospital h
LEFT JOIN victima v ON h.id_hospital = v.id_hospital
LEFT JOIN victima_asociado va ON v.id_victima = va.id_victima
LEFT JOIN asociado a ON va.id_asociado = a.id_asociado
LEFT JOIN tipo_contacto t ON va.id_victima_asociado = t.id_victima_asociado AND t.contacto_fisico IS NOT NULL
GROUP BY h.nombre_hospital, t.contacto_fisico
ORDER BY h.nombre_hospital, porcentaje_victimas DESC;



