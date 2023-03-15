
const port = process.env.port || 3000,
express = require('express'),
cors = require('cors'),
bodyParser= require('body-parser'),
app= express();
db= require('./models')

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

app.listen(port, () => {
    console.log(`Servidor corriendo en puerto ${port}...`);
});

db.sequelize
.sync({force:true})
.then(()=> console.log('Conectado a la base de datos'))
.catch((e)=> console.log(`Error => ${e}`))