CREATE TABLE victima_asociado (
    id_victima NUMBER DEFAULT secuencia_id.NEXTVAL,
    id_asociado NUMBER DEFAULT secuencia_id.NEXTVAL,
    FOREIGN KEY (id_victima) REFERENCES victima(id_victima),
    FOREIGN KEY (id_asociado) REFERENCES asociado(id_asociado)
);


ALTER TABLE tratamiento 
ADD CONSTRAINT pk_id_TRATAMIENTO PRIMARY KEY (id_TRATAMIENTO);

ALTER TABLE TRATAMIENTO ADD id_TRATAMIENTO NUMBER DEFAULT secuencia_id.NEXTVAL PRIMARY KEY;
ALTER TABLE TRATAMIENTO ADD id_VICTIMA NUMBER DEFAULT secuencia_id.NEXTVAL ;

ALTER TABLE TRATAMIENTO
ADD CONSTRAINT fk_victima_TRATAMIENTO FOREIGN KEY (id_VICTIMA) REFERENCES VICTIMA(id_VICTIMA);

ALTER TABLE tabla_hija
ADD CONSTRAINT fk_tabla_hija_tabla_padre FOREIGN KEY (id_padre) REFERENCES tabla_padre(id);

INSERT INTO Victima(NOMBRE_VICTIMA,APELLIDO_VICTIMA, DIRECCION_VICTIMA,FECHA_PRIMERA_SOSPECHA,FECHA_CONFIRMACION,FECHA_MUERTE,ESTADO_VICTIMA)
VALUES ('juan','de leon', 'manzana31',TO_DATE('11/02/2021  00:00:00', 'DD-MM-YYYY HH24:MI:SS'),TO_DATE('11/02/2021  00:00:00', 'DD-MM-YYYY HH24:MI:SS '),TO_DATE('11/02/2021  00:00:00', 'DD-MM-YYYY HH24:MI:SS'),'bien jej');

truncate table tabla_temporal
truncate table hospital
truncate table victima
truncate table ubicacion

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



INSERT INTO HOSPITAL (ID_HOSPITAL, NOMBRE_HOSPITAL, DIRECCION_HOSPITAL)
SELECT ID_TABLA_TEMPORAL, NOMBRE_HOSPITAL, DIRECCION_HOSPITAL
FROM (
  SELECT ROW_NUMBER() OVER (PARTITION BY NOMBRE_HOSPITAL ORDER BY ID_TABLA_TEMPORAL) AS RN,
         ID_TABLA_TEMPORAL,
         NOMBRE_HOSPITAL,
         DIRECCION_HOSPITAL
  FROM TABLA_TEMPORAL
  WHERE NOMBRE_HOSPITAL IS NOT NULL
) T
WHERE RN = 1;

INSERT INTO VICTIMA (ID_VICTIMA, ID_HOSPITAL, NOMBRE_VICTIMA, APELLIDO_VICTIMA, DIRECCION_VICTIMA, FECHA_CONFIRMACION, FECHA_MUERTE, ESTADO_VICTIMA, FECHA_PRIMERA_SOSPECHA)
SELECT ID_TABLA_TEMPORAL, HOSPITAL.ID_HOSPITAL, NOMBRE_VICTIMA, APELLIDO_VICTIMA, DIRECCION_VICTIMA, FECHA_CONFIRMACION, FECHA_MUERTE, ESTADO_VICTIMA, FECHA_PRIMERA_SOSPECHA
FROM (
  SELECT ROW_NUMBER() OVER (PARTITION BY NOMBRE_VICTIMA ORDER BY ID_TABLA_TEMPORAL) RN, ID_TABLA_TEMPORAL, NOMBRE_VICTIMA, APELLIDO_VICTIMA, DIRECCION_VICTIMA, FECHA_CONFIRMACION, FECHA_MUERTE, ESTADO_VICTIMA, FECHA_PRIMERA_SOSPECHA
  FROM TABLA_TEMPORAL
  WHERE NOMBRE_VICTIMA IS NOT NULL
) T
LEFT JOIN HOSPITAL ON T.ID_TABLA_TEMPORAL = HOSPITAL.ID_HOSPITAL
WHERE RN = 1;


INSERT INTO ubicacion (id_victima, ubicacion_victima, fecha_llegada, fecha_retiro)
SELECT v.id_victima, t.ubicacion_victima, t.fecha_llegada, t.fecha_retiro
FROM victima v
INNER JOIN tabla_temporal t ON v.nombre_victima = t.nombre_victima
WHERE t.ubicacion_victima IS NOT NULL AND v.id_victima IN (SELECT id_victima FROM victima);


SELECT id_victima, ubicacion_victima, fecha_llegada, fecha_retiro, COUNT(*) as cantidad
FROM ubicacion
GROUP BY id_victima, ubicacion_victima, fecha_llegada, fecha_retiro
HAVING COUNT(*) > 1;

DELETE FROM ubicacion
WHERE id_ubicacion NOT IN (
    SELECT MIN(id_ubicacion)
    FROM ubicacion
    GROUP BY id_victima, ubicacion_victima, fecha_llegada, fecha_retiro
);


INSERT INTO tratamiento (id_victima, tratamiento, fecha_inicio_tratamiento, fecha_fin_tratamiento, efectividad_en_victima,efectividad)
SELECT v.id_victima, t.tratamiento, t.fecha_inicio_tratamiento, t.fecha_fin_tratamiento, t.efectividad_en_victima,efectividad
FROM victima v
INNER JOIN tabla_temporal t ON v.nombre_victima = t.nombre_victima
WHERE t.tratamiento IS NOT NULL AND v.id_victima IN (SELECT id_victima FROM victima);

SELECT id_victima, tratamiento, fecha_inicio_tratamiento, fecha_fin_tratamiento, efectividad_en_victima,efectividad, COUNT(*) as cantidad
FROM TRATAMIENTO
GROUP BY id_victima, tratamiento, fecha_inicio_tratamiento, fecha_fin_tratamiento, efectividad_en_victima,efectividad
HAVING COUNT(*) > 1;

DELETE FROM TRATAMIENTO
WHERE id_TRATAMIENTO NOT IN (
    SELECT MIN(id_TRATAMIENTO)
    FROM TRATAMIENTO
    GROUP BY id_victima, tratamiento, fecha_inicio_tratamiento, fecha_fin_tratamiento, efectividad_en_victima,efectividad
);




