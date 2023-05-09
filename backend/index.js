
const port = process.env.port || 3000,
express = require('express'),
cors = require('cors'),
bodyParser= require('body-parser'),
app= express();
db= require('./models')
const fs = require('fs');
const oracledb = require('oracledb');
const { exec } = require('child_process');
const path = require('path');

const { Sequelize } = require('sequelize');
const sequelize = new Sequelize({
  dialect: 'oracle',
  database: 'xe',
  username: 'system',
  password: '42058019',
  host: 'localhost',
  port: 1521,
});

app.use(cors()) 
app.use(bodyParser.json())
app.use(bodyParser.urlencoded({extended:false}))

app.post('/consulta1', async (req, res) => {
  const consulta = `SELECT 
  ROW_NUMBER() OVER (ORDER BY h.nombre_hospital, h.direccion_hospital) AS numero_fila,
  h.nombre_hospital, 
  h.direccion_hospital, 
  COUNT(v.id_victima) AS fallecidos,
  (SELECT COUNT(DISTINCT h2.id_hospital) FROM hospital h2 INNER JOIN victima v2 ON h2.id_hospital = v2.id_hospital WHERE v2.fecha_muerte IS NOT NULL) AS total_hospitales_fallecidos
FROM hospital h
LEFT JOIN victima v ON h.id_hospital = v.id_hospital
WHERE v.fecha_muerte IS NOT NULL
GROUP BY h.nombre_hospital, h.direccion_hospital;
`;
  try {
    const result = await sequelize.query(consulta);
    console.log(result);
    res.json(result[0]); // Enviamos el resultado como un JSON
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al ejecutar la consulta.');
  }
});

app.post('/consulta2', async (req, res) => {
  const consulta = `SELECT v.nombre_victima, v.apellido_victima, t.tratamiento, 
  COUNT(*) OVER() AS total_filas
FROM VICTIMA v
JOIN TRATAMIENTO t ON v.id_victima = t.id_victima
WHERE t.tratamiento = 'Transfusiones de sangre'
AND t.efectividad_en_victima > 5;

  `;
  try {
    const result = await sequelize.query(consulta);
    console.log(result);
    res.json(result[0]); // Enviamos el resultado como un JSON
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al ejecutar la consulta.');
  }
});

app.post('/consulta3', async (req, res) => {
  const consulta = `SELECT ROW_NUMBER() OVER (ORDER BY v.nombre_victima) AS numero_fila,
  v.nombre_victima,
  v.apellido_victima,
  v.direccion_victima,
  COUNT(va.id_asociado) AS num_asociados
FROM victima v
JOIN victima_asociado va ON v.id_victima = va.id_victima
JOIN asociado a ON va.id_asociado = a.id_asociado
WHERE v.fecha_muerte IS NOT NULL
GROUP BY v.nombre_victima, v.apellido_victima, v.direccion_victima
HAVING COUNT(va.id_asociado) > 3
ORDER BY v.nombre_victima;
`;
  try {
    const result = await sequelize.query(consulta);
    console.log(result);
    res.json(result[0]); // Enviamos el resultado como un JSON
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al ejecutar la consulta.');
  }
});

app.post('/consulta4', async (req, res) => {
  const consulta = `SELECT v.nombre_victima, v.apellido_victima
  FROM victima v
  JOIN victima_asociado va ON v.id_victima = va.id_victima
  JOIN tipo_contacto tc ON va.id_victima_asociado = tc.id_victima_asociado
  WHERE v.estado_victima = 'Suspendida' AND tc.contacto_fisico = 'Beso'
  GROUP BY v.nombre_victima, v.apellido_victima
  HAVING COUNT(DISTINCT va.id_asociado) > 2;`;
  try {
    const result = await sequelize.query(consulta);
    console.log(result);
    res.json(result[0]); // Enviamos el resultado como un JSON
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al ejecutar la consulta.');
  }
});

app.post('/consulta5', async (req, res) => {
  const consulta = `SELECT v.nombre_victima, COUNT(*) as cantidad_tratamientos
  FROM victima v
  JOIN tratamiento t ON v.id_victima = t.id_victima
  WHERE t.tratamiento = 'Oxigeno'
  GROUP BY v.nombre_victima
  ORDER BY cantidad_tratamientos DESC
  FETCH FIRST 5 ROWS ONLY;`;
  try {
    const result = await sequelize.query(consulta);
    console.log(result);
    res.json(result[0]); // Enviamos el resultado como un JSON
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al ejecutar la consulta.');
  }
});

app.post('/consulta6', async (req, res) => {
  const consulta = `SELECT v.nombre_victima, v.apellido_victima, v.fecha_muerte 
  FROM victima v 
  JOIN tratamiento t ON v.id_victima = t.id_victima 
  JOIN ubicacion u ON v.id_victima = u.id_victima 
  WHERE u.ubicacion_victima = '1987 Delphine Well' 
  AND t.tratamiento = 'Manejo de la presion arterial'
  AND v.fecha_muerte IS NOT NULL;`;
  try {
    const result = await sequelize.query(consulta);
    console.log(result);
    res.json(result[0]); // Enviamos el resultado como un JSON
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al ejecutar la consulta.');
  }
});


app.post('/consulta7', async (req, res) => {
  const consulta = `SELECT 
  ROW_NUMBER() OVER (ORDER BY v.nombre_victima, v.apellido_victima) AS numero_fila,
  v.nombre_victima, 
  v.apellido_victima, 
  v.direccion_victima 
FROM 
  Victima v
  JOIN Victima_Asociado va ON v.id_victima = va.id_victima
  JOIN Asociado a ON va.id_asociado = a.id_asociado
  JOIN Hospital h ON v.id_hospital = h.id_hospital
  JOIN Tratamiento t ON v.id_victima = t.id_victima
GROUP BY 
  v.nombre_victima, 
  v.apellido_victima, 
  v.direccion_victima
HAVING 
  COUNT(DISTINCT va.id_asociado) < 2 AND 
  COUNT(DISTINCT t.id_tratamiento) = 2
ORDER BY 
  v.nombre_victima, 
  v.apellido_victima;
`;
  try {
    const result = await sequelize.query(consulta);
    console.log(result);
    res.json(result[0]); // Enviamos el resultado como un JSON
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al ejecutar la consulta.');
  }
});


app.post('/consulta8', async (req, res) => {
  const consulta = `SELECT 
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
  total_tratamientos DESC;`;
  try {
    const result = await sequelize.query(consulta);
    console.log(result);
    res.json(result[0]); // Enviamos el resultado como un JSON
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al ejecutar la consulta.');
  }
});


app.post('/consulta9', async (req, res) => {
  const consulta = `SELECT h.nombre_hospital, 
  ROW_NUMBER() OVER (ORDER BY COUNT(v.id_victima) DESC) AS fila,
  TO_CHAR(COUNT(v.id_victima) / SUM(COUNT(v.id_victima)) OVER() * 100, 'FM999.00') AS porcentaje_victimas
FROM hospital h
LEFT JOIN victima v ON h.id_hospital = v.id_hospital
GROUP BY h.nombre_hospital
ORDER BY COUNT(v.id_victima) DESC;

`;
  try {
    const result = await sequelize.query(consulta);
    console.log(result);
    res.json(result[0]); // Enviamos el resultado como un JSON
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al ejecutar la consulta.');
  }
});

app.post('/consulta10', async (req, res) => {
  const consulta = `SELECT h.nombre_hospital, t.contacto_fisico, 
  COUNT(*) / SUM(COUNT(*)) OVER(PARTITION BY h.id_hospital) * 100 AS porcentaje_victimas
FROM hospital h
LEFT JOIN victima v ON h.id_hospital = v.id_hospital
LEFT JOIN victima_asociado va ON v.id_victima = va.id_victima
LEFT JOIN asociado a ON va.id_asociado = a.id_asociado
LEFT JOIN tipo_contacto t ON va.id_victima_asociado = t.id_victima_asociado AND t.contacto_fisico IS NOT NULL
GROUP BY h.nombre_hospital, t.contacto_fisico
ORDER BY h.nombre_hospital, porcentaje_victimas DESC

`;
  try {
    const result = await sequelize.query(consulta);
    console.log(result);
    res.json(result[0]); // Enviamos el resultado como un JSON
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al ejecutar la consulta.');
  }
});


app.post('/cargarModelo', async (req, res) => {
  const consulta= `CREATE TABLE "SYSTEM"."HOSPITAL" 
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
TABLESPACE "SYSTEM" ;`
  const consulta2 = `  
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
`;

const consulta3= `CREATE TABLE "SYSTEM"."UBICACION" 
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
TABLESPACE "SYSTEM" ;`

const consulta4= `  CREATE TABLE "SYSTEM"."TRATAMIENTO" 
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
TABLESPACE "SYSTEM" ;`

const consulta5=`  CREATE TABLE "SYSTEM"."ASOCIADO" 
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
TABLESPACE "SYSTEM" ;`

const consulta6 =`  CREATE TABLE "SYSTEM"."VICTIMA_ASOCIADO" 
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
TABLESPACE "SYSTEM" ;`

const consulta7=`CREATE TABLE "SYSTEM"."TIPO_CONTACTO" 
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
TABLESPACE "SYSTEM" ;`

const consulta8 = `BEGIN migrar(); END;`;

const consulta9=`DELETE FROM hospital
WHERE id_hospital NOT IN (
    SELECT MIN(id_hospital)
    FROM hospital
    GROUP BY  nombre_hospital, direccion_hospital
);`

const consulta10 =`BEGIN migrar_victima(); END;`

const consulta11=`DELETE FROM victima
WHERE id_victima NOT IN (
    SELECT MIN(id_victima)
    FROM victima
    GROUP BY  NOMBRE_VICTIMA, APELLIDO_VICTIMA, DIRECCION_VICTIMA, FECHA_CONFIRMACION, FECHA_MUERTE, ESTADO_VICTIMA, FECHA_PRIMERA_SOSPECHA, id_hospital
);`

const consulta12 =`BEGIN migrar_ubicacion(); END;`

const consulta13 =`DELETE FROM ubicacion
WHERE id_ubicacion NOT IN (
    SELECT MIN(id_ubicacion)
    FROM ubicacion
    GROUP BY id_victima, ubicacion_victima, fecha_llegada, fecha_retiro
);`

const consulta14 =`BEGIN migrar_tratamiento(); END;`

const consulta15 =`DELETE FROM TRATAMIENTO
WHERE id_TRATAMIENTO NOT IN (
    SELECT MIN(id_TRATAMIENTO)
    FROM TRATAMIENTO
    GROUP BY id_victima, tratamiento, fecha_inicio_tratamiento, fecha_fin_tratamiento, efectividad_en_victima,efectividad
);`

const consulta16 =`BEGIN migrar_asociado(); END;`

const consulta17 =`DELETE FROM asociado
WHERE id_asociado NOT IN (
    SELECT MIN(id_asociado)
    FROM asociado
    GROUP BY nombre_asociado, apellido_asociado, fecha_conocio
);`

const consulta18 =`INSERT INTO victima_asociado ( id_asociado, id_victima, nombre_victima)
SELECT a.id_asociado, v.id_victima , v.nombre_victima
FROM victima v
INNER JOIN tabla_temporal t ON v.nombre_victima = t.nombre_victima
INNER JOIN asociado a ON a.nombre_asociado = t.nombre_asociado AND a.apellido_asociado = t.apellido_asociado and t.fecha_conocio=a.fecha_conocio
WHERE v.id_victima IN (SELECT id_victima FROM victima);`

const consulta19 =`DELETE FROM victima_asociado
WHERE id_victima_asociado NOT IN (
    SELECT MIN(id_victima_asociado)
    FROM victima_asociado
    GROUP BY id_asociado, id_victima
);`

const consulta20 =`BEGIN migrar_tipo_contacto(); END;`

const consulta21 =`DELETE FROM tipo_contacto
WHERE id_contacto NOT IN (
    SELECT MIN(id_contacto)
    FROM tipo_contacto
    GROUP BY id_victima_asociado, contacto_fisico, fecha_inicio_contacto, fecha_fin_contacto
);`

  try {
    const result = await sequelize.query(consulta);
    const result2 = await sequelize.query(consulta2);
    const result3 = await sequelize.query(consulta3);
    const result4 = await sequelize.query(consulta4);
    const result5 = await sequelize.query(consulta5);
    const result6 = await sequelize.query(consulta6);
    const result7 = await sequelize.query(consulta7);
    const result8 = await sequelize.query(consulta8);
    const result9 = await sequelize.query(consulta9);
    const result10 = await sequelize.query(consulta10);
    const result11 = await sequelize.query(consulta11);
    const result12 = await sequelize.query(consulta12);
    const result13 = await sequelize.query(consulta13);
    const result14 = await sequelize.query(consulta14);
    const result15 = await sequelize.query(consulta15);
    const result16 = await sequelize.query(consulta16);
    const result17 = await sequelize.query(consulta17);
    const result18 = await sequelize.query(consulta18);
    const result19 = await sequelize.query(consulta19);
    const result20 = await sequelize.query(consulta20);
    const result21 = await sequelize.query(consulta21);
    
    //console.log(result);
    res.send('Carga de modelo realizada correctamente.');
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al ejecutar la consulta.');
  }
});


app.post('/eliminarModelo', async (req, res) => {
    const consulta = `drop table tipo_contacto;
                      drop table tratamiento;
                      drop table ubicacion;
                      drop table victima_asociado;
                      drop table asociado;
                      drop table victima;
                      drop table hospital;`
  
    try {
      const result = await sequelize.query(consulta);
      console.log(result); 
      res.send('Tablas del modelo de datos eliminadas.');
    } catch (error) {
      console.error(error);
      res.status(500).send('Error al ejecutar la consulta.');
    }
});

app.post('/cargaTemporal', async (req, res) => {
  process.chdir('C:\\Users\\sebas');
  const command = `sqlldr userid=system/42058019 control='C:\\Users\\sebas\\OneDrive\\Documentos\\Primer Semestre 2023\\Bases 1\\Lab\\Proyectos\\cargas\\DB_Excel.ctl' data='C:\\Users\\sebas\\OneDrive\\Documentos\\Primer Semestre 2023\\Bases 1\\Lab\\Proyectos\\DB_Excel.csv' log='C:\\Users\\sebas\\OneDrive\\Documentos\\Primer Semestre 2023\\Bases 1\\Lab\\Proyectos\\DB_Excel.log' bad='C:\\Users\\sebas\\OneDrive\\Documentos\\Primer Semestre 2023\\Bases 1\\Lab\\Proyectos\\maloo.bad'`;

  exec(command, (error, stdout, stderr) => {
    if (error) {
      console.error(`exec error: ${error}`);
      res.status(500).send('Error al ejecutar SQL*Loader 1');
      return;
    }

    if (stderr) {
      console.error(`stderr: ${stderr}`);
      res.status(500).send('Error al ejecutar SQL*Loader 2');
      return;
    }

    console.log(`stdout: ${stdout}`);
    res.status(200).send('Carga de datos a tabla temporal finalizada correctamente');
  });
});

app.post('/eliminarTemporal', async (req, res) => {
  const consulta = `TRUNCATE TABLE tabla_temporal;`

  try {
    const result = await sequelize.query(consulta);
    console.log(result); 
    res.send('Tablas del modelo de datos eliminadas.');
  } catch (error) {
    console.error(error);
    res.status(500).send('Error al ejecutar la consulta.');
  }
});


app.listen(port, () => {
    console.log(`Servidor corriendo en puerto ${port}...`);
});

db.sequelize
.sync({force:true})
.then(()=> console.log('Conectado a la base de datos'))
.catch((e)=> console.log(`Error => ${e}`))