/**
 *  @Author:    chenrongxin
 *  @Create Date:   2017-11-09
 *  @Description:   融资融券数据
 */
var mongoose = require('./db'),
    Schema = mongoose.Schema,
    moment = require('moment')

var rzrqSchema = new Schema({  
    code : {type:String},//代码 
    xh : {type:String},//序号
    jysj : {type:String},//交易时间
    rzye : {type:String},//融资余额
    rzmr : {type:String},//融资买入
    rzch : {type:String},//融资偿还
    rzjmr : {type:String},//融资净买入
    rqyl : {type:String},//融券余量
    rqmc : {type:String},//融券卖出
    rqch : {type:String},//融券偿还
    rqjmc : {type:String},//融券净卖出
    rzrqye : {type:String},//融资融券余额
    insert_ime : {type : String, default : moment().format('YYYY-MM-DD HH:mm:ss') },     //申请时间 
    insert_timeStamp : {type : String,default:moment().format('X')}
})

module.exports = mongoose.model('rzrq',rzrqSchema);