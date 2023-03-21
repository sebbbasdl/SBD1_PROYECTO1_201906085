
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
  const consulta = `SELECT h.nombre_hospital, h.direccion_hospital, COUNT(v.id_victima) AS fallecidos,
  (SELECT COUNT(DISTINCT h2.id_hospital) FROM hospital h2 INNER JOIN victima v2 ON h2.id_hospital = v2.id_hospital WHERE v2.fecha_muerte IS NOT NULL) AS total_hospitales_fallecidos
  FROM hospital h
  LEFT JOIN victima v ON h.id_hospital = v.id_hospital
  WHERE v.fecha_muerte IS NOT NULL
  GROUP BY h.nombre_hospital, h.direccion_hospital;`;
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
  const consulta = `
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
`;
  try {
    const result = await sequelize.query(consulta);
    console.log(result);
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



app.listen(port, () => {
    console.log(`Servidor corriendo en puerto ${port}...`);
});

db.sequelize
.sync({force:true})
.then(()=> console.log('Conectado a la base de datos'))
.catch((e)=> console.log(`Error => ${e}`))