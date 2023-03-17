
const port = process.env.port || 3000,
express = require('express'),
cors = require('cors'),
bodyParser= require('body-parser'),
app= express();
db= require('./models')
const fs = require('fs');
const oracledb = require('oracledb');

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
    const consulta = `INSERT INTO hospital (nombre_hosp, direccion_hosp) VALUES ('San Juan', 'Avenida Elena')`
  
    try {
      const result = await sequelize.query(consulta);
      console.log(result); 
      res.send('Consulta ejecutada correctamente.');
    } catch (error) {
      console.error(error);
      res.status(500).send('Error al ejecutar la consulta.');
    }
});


app.post('/consulta2', async (req, res) => {
  try {
    // Leer el archivo CSV
    const csvData = fs.readFileSync('C:\\Users\\sebas\\OneDrive\\Documentos\\Primer Semestre 2023\\Bases 1\\Lab\\Proyectos\\DB_ExcelV.csv', 'utf8');

    // Convertir los datos del CSV en un array de objetos
    const hospitales = csvData.split('\n').slice(1).map(line => {
      const [nombre_hospital, direccion_hospital] = line.split(';');
      return { nombre_hospital, direccion_hospital };
    });

    // Función asincrónica para insertar cada hospital en la base de datos
    async function insertarHospital(hospital) {
      let connection;
      const consulta = 'INSERT INTO auxhospital (nombre_hospital, direccion_hospital) VALUES (:nombre_hospital, :direccion_hospital)'
      try {
        const result = await sequelize.query(consulta, {
          replacements: {
            nombre_hospital: hospital.nombre_hospital,
            direccion_hospital: hospital.direccion_hospital
          }
        });
        console.log(result); 
        res.send('Consulta ejecutada correctamente.');
      } catch (error) {
        console.error(error);
        res.status(500).send('Error al ejecutar la consulta.');
      }
    }

    // Iterar sobre cada hospital y hacer la inserción en la base de datos
    for (const hospital of hospitales) {
      await insertarHospital(hospital);
    }

    res.status(200).send('Inserción de hospitals completada correctamente');
  } catch (err) {
    console.error(`Error al insertar las hospitals: ${err}`);
    res.status(500).send('Error al insertar las hospitals');
  }
});

app.listen(port, () => {
    console.log(`Servidor corriendo en puerto ${port}...`);
});

db.sequelize
.sync({force:true})
.then(()=> console.log('Conectado a la base de datos'))
.catch((e)=> console.log(`Error => ${e}`))