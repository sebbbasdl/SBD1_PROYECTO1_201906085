const sequelize = require("sequelize-oracle");



async function crearHospital(){
    const query = `INSERT INTO hospital (nombre, Direccion) VALUES ('San Juan', 'Avenida Elena')`
    
    const resultado = await sequelize.query(query);
    console.log(resultado);
    return resultado
        
}

module.exports={
    crearHospital
}