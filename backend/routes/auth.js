
const express = require('express'),
router=express.Router(),
{_crearHospital} = require('../controllers/control_consulta.js');

router.post('/consulta1',async(req,res)=>{
    const consulta = `INSERT INTO hospital (nombre_hosp, direccion_hosp) VALUES ('San Juan', 'Avenida Elena')`
  
    try {
      const result = await sequelize.query(consulta);
      console.log(result); 
      res.send('Consulta ejecutada correctamente.');
    } catch (error) {
      console.error(error);
      res.status(500).send('Error al ejecutar la consulta.');
    }
} );


module.exports=router