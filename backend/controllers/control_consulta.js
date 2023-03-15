const {crearHospital}= require ('../services/consulta.js')

async function _crearHospital(){
    return await crearHospital()
}

module.exports={ _crearHospital}
