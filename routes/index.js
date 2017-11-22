var express = require('express');
var router = express.Router();
const logic = require('../logic/logic')
const logger = require('../log/logConfig').logger

/* GET home page. */
router.get('/', function(req, res, next) {
	logger.info('------------------------ get / ------------------------')
	logic.rongzi(function(error,result){
		if(error){
			return res.json({'errCode':-1,'errMsg':error})
		}
		return res.render('index', { title: 'Express' });
	})
});

//rzrq
router.get('/rzrq',function(req,res){
	//let code = '601601'
	logic.getRzrqData(function(error,result){
		if(error){
			console.log('--------------------------- rzrq router error ---------------------------')
			res.json({'errCode':-1,'errMsg':error})
		}
		res.render('rzrq',{title:'rzrq','errCode':0,'result':result})
	})
})

module.exports = router;
