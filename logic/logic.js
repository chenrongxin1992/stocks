const charset = require('superagent-charset')
const superagent = charset(require('superagent'))
const request = require('request')
const cheerio =require('cheerio')
const async = require('async')
const schedule = require("node-schedule")

const rzrq = require('../db/rzrq')

var zongshu = null

const domain = 'http://data.10jqka.com.cn' //域名
//const code_url = 'http://data.10jqka.com.cn/market/rzrq/' //获取code链接

//获取所有的code(rongzi)
function fetchCodeArr(callback){
	let code_arr = [],
		temp_obj = {}
	async.waterfall([
		function(cb){
			let get_code_url = domain + '/market/rzrq/'
			console.log('获取code页面链接-->',get_code_url)
			superagent
				.get(get_code_url)
				.charset('gbk')
				.end(function(err,res){
					if(err){
						console.log('---------------------- get_code_url err ----------------------')
						console.log(err)
						cb(err)
					}
					if(res.status === 200){
						let $ = cheerio.load(res.text)
						//获取页面数
						let pagenum = $('.page_info').text().split('/')[2]
						console.log($('.page_info').text())
						console.log('code 页面数-->',pagenum)
										 //http://data.10jqka.com.cn/market/rzrq/board/ls/field/rzjmr/order/desc/page/19/ajax/1
						//获取code的链接 http://data.10jqka.com.cn/market/rzrq/board/ls/field/rzjmr/order/desc/page/3/ajax/1/
						let code_url_arr = []
						for(let i=1;i<=parseInt(pagenum);i++){
							temp_url = domain + '/market/rzrq/board/ls/field/rzjmr/order/desc/page/' + i + '/ajax/1'
							code_url_arr.push(temp_url)
						}
						console.log('待爬取链接-->',code_url_arr)
						cb(null,code_url_arr)
					}
				})
		},
		function(code_url_arr,cb){
			async.eachLimit(code_url_arr,1,function(item,cbb){
				console.log('当前爬取链接-->',item)
				superagent
					.get(item)
					.charset('gbk')
					.end(function(err,res){
						if(err){
							console.log('-------------------------- fetch code_url_arr err --------------------------')
							console.log(err)
							cbb(err)
						}
						if(res.status === 200){
							let $ = cheerio.load(res.text)
							//获取所有的 tbody tr
							let tbody_tr = $('tbody').find('tr'),
								tbody_tr_length = tbody_tr.length
							console.log('本页记录条数-->',tbody_tr_length)

							let tbody_tr_td = $('tbody').find('tr').children('td'),
								tbody_tr_td_length = tbody_tr_td.length
							console.log('总的td数-->',tbody_tr_td_length)
							console.log('--------------------------- what happen ---------------------------')
							tbody_tr_td.each(function(i,it){
								switch(i%13){
									case 0:
										//console.log(it)
										temp_obj.xh = it.children[0].data
										break
									case 1:
										temp_obj.code = $(it).children()[0].children[0].data
										break
									case 2:
										temp_obj.name = $(it).children()[0].children[0].data
										break
									case 3:
										temp_obj.rzye = formatNum(it.children[0].data)
										break
									case 4:
										temp_obj.rzmr = formatNum(it.children[0].data)
										break
									case 5:
										temp_obj.rzch = formatNum(it.children[0].data)
										break
									case 6:
										temp_obj.rzjmr = formatNum(it.children[0].data)
										break
									case 7:
										temp_obj.rqyl = it.children[0].data
										break
									case 8:
										temp_obj.rqmc = it.children[0].data
										break
									case 9:
										temp_obj.rqch = it.children[0].data
										break
									case 10:
										temp_obj.rzjmc = it.children[0].data
										break
									case 11:
										temp_obj.rzrqye = formatNum(it.children[0].data)
										break
									case 12:
										temp_obj.his = $(it).children()[0].children[0].data
										code_arr.push(temp_obj)
										temp_obj = {}
										break
								}
							})
							console.log('--------------------------- what happen ---------------------------')
							console.log('code_arr length ------------------------------>',code_arr.length)
							cbb()
						}
					})
			},function(err){
				if(err){
					console.log('------------------------ eachLimit err --------------------------')
					console.log(err)
					cb(err)
				}
				console.log(code_arr)
				cb(null,code_arr)
			})
		}
	],function(error,result){
		if(error){
			console.log('-------------------------- async err ---------------------------')
			console.log(error)
			return callback(error)
		}
		return callback(null,result)
	})
}

function fetchData(code,callback){
	console.log('----- in fetchData function -----')
	let res_arr = new Array(),
		temp_obj = {}
	async.waterfall([
		function(cb){
			
			//数据来源
			//http://data.10jqka.com.cn/market/rzrqgg/code/601318/order/desc/page/1/ajax/1/
			//let url = 'http://data.10jqka.com.cn/market/rzrqgg/code/601318/'
			let url = domain + '/market/rzrqgg/code/' + code + '/'
			console.log('获取页面链接-->',url)
			superagent
				.get(url)
				.charset('gbk')
				.end(function(err,res){
					if(err){
						console.log('----- 获取页数失败 -----')
						console.log('状态码-->',response.status)
						console.log(err)
					}
					if(res.status === 200){
						let $ = cheerio.load(res.text)
						//获取数据总页数
						let pagenum = $('.page_info').text().split('/')[1]
						console.log('页数总共有-->',pagenum)

						//构造数据来源链接
						let data_url_arr = new Array()
						for(let i=1;i<parseInt(pagenum)+1;i++){
							//temp_url = domain + '/market/rzrqgg/code/601318/order/desc/page/' + i + '/ajax/1'
							temp_url = domain + '/market/rzrqgg/code/' + code +'/order/desc/page/' + i + '/ajax/1'
							data_url_arr.push(temp_url) 
						}
						console.log('待爬取链接-->',data_url_arr)
						cb(null,data_url_arr)
					}
				})
		},
		function(arg,cb){
			//console.time('抓取数据耗时-->')
			async.eachLimit(arg,1,function(item,cbb){
				console.log('当前爬取-->',item)
				superagent
					.get(item)
					.charset('gbk')
					.end(function(err,response){
						if(err){
							console.log('----- 抓取时错误 -----')
							console.log('状态码-->',response.status)
							console.log(err)
							cbb(err)
						}
						if(response.status === 200){
							let $ = cheerio.load(response.text)
							//获取所有的 tbody tr
							let tbody_tr = $('tbody').find('tr'),
								tbody_tr_length = tbody_tr.length
							console.log('本页记录条数-->',tbody_tr_length)

							let tbody_tr_td = $('tbody').find('tr').children('td'),
								tbody_tr_td_length = tbody_tr_td.length
							console.log('总的td数-->',tbody_tr_td_length)

							//let res_arr = new Array(),
							//	temp_obj = {}
							tbody_tr_td.each(function(i,it){
								switch(i%11){
									case 0:
										//console.log(it)
										temp_obj.xh = it.children[0].data
										break
									case 1:
										temp_obj.jysj = it.children[0].data.trim()
										break
									case 2:
										temp_obj.rzye = formatNum(it.children[0].data)
										break
									case 3:
										temp_obj.rzmr = formatNum(it.children[0].data)
										break
									case 4:
										temp_obj.rzch = formatNum(it.children[0].data)
										break
									case 5:
										temp_obj.rzjmr = formatNum(it.children[0].data)
										break
									case 6:
										temp_obj.rqyl = formatNum(it.children[0].data)
										break
									case 7:
										temp_obj.rqmc = it.children[0].data
										break
									case 8:
										temp_obj.rqch = it.children[0].data
										break
									case 9:
										temp_obj.rqjmc = it.children[0].data
										break
									case 10:
										temp_obj.rzrqye = formatNum(it.children[0].data)
										res_arr.push(temp_obj)
										temp_obj = {}
										break
								}
							})
							zongshu = parseInt(zongshu) + 1
							cbb(null)
						}
					})
			},function(error){
				if(error){
					console.log('----- eachLimit error -----')
					console.log(error)
					cb(error)
				}
				//console.timeEnd('抓取数据耗时-->')
				cb(null,res_arr)
			})
		},
		function(arg,cb){
			//console.time('查询和删除耗时-->')
			let search = rzrq.find({code:code})
				search.exec(function(err,docs){
					if(err){
						console.log('----- search err -----')
						console.log(err)
						cb(err)
					}
					rzrq.remove({code:code},function(er,doc){
						if(er){
							console.log('----- remove records err -----')
							console.log(er)
							cb(er)
						}
						console.log('----- remove success -----')
						console.log('对应代码 -->',code)
						//console.timeEnd('查询和删除耗时-->')
						cb(null,arg)
					})
				})
		},
		function(arg,cb){
			//console.time('插入耗时-->')
			console.log('数据记录数-->',arg.length)
			async.eachLimit(arg,100,function(item,cbbb){
				let insert_rzrq = new rzrq({
					code : code,
					xh : item.xh,
					jysj : item.jysj,
					rzye : item.rzye,
					rzmr : item.rzmr,
					rzch : item.rzch,
					rzjmr : item.rzjmr,
					rqyl : item.rqyl,
					rqmc : item.rqmc,
					rqch : item.rqch,
					rqjmc : item.rqjmc,
					rzrqye : item.rzrqye
				})
				insert_rzrq.save(function(err){
					if(err){
						console.log('----- save err -----')
						console.log(err)
						cbbb(err)
					}
					cbbb(null)
				})
			},function(err){
				if(err){
					console.log('----- eachLimit save err -----')
					cb(err)
				}
				//console.timeEnd('插入耗时-->')
				cb(null,arg)
			})
		}
	],function(error,result){
		if(error){
			console.log('----- async waterfall error -----')
			console.log(error)
			return callback(error)
		}
		//console.log('爬取结果-->',result)
		//return callback(result)
		return callback(null)
	})
}

/*作用与eachSeries类似，最后也只能收到err/null作为反馈，但与eachSeries串行执行不同，each是全部执行并等待全部完成后返回，中间如果有错误就不等全部完成就立即返回。
    可以看到，函数名中有each的是没有结果集的，函数明名map是有结果集的；
也可以看到,函数都是只要iterator返回err就立即调用callback返回。
并且其内部有控制过，callback只会被调用一次，如果你发现被回调了多次，那么这一定是一个bug，可以向作者反馈。*/

exports.execFetchData = function (){
	console.time('process costs time ----------------------------->')
	async.waterfall([
		function(cb){
			console.time('get code_arr costs time ------------------------>')
			fetchCodeArr(function(err,res){
				if(err){
					console.log('-------------------------- 调用 fetchCodeArr err --------------------------')
					console.log(err)
				}
				console.timeEnd('get code_arr costs time ------------------------>')
				cb(null,res)
			})
		},
		function(arg,cb){
			let date = new Date(2017,10,11,13,08,00);

			//5分钟执行一次
			console.time('fetchData costs time ------------------------------->')
			schedule.scheduleJob('06 17 * * *', function(){  //每天九点11分
				async.eachLimit(arg,1,function(item,cbb){
					//console.time('time')
					fetchData(item.code,function(er,re){
						if(er){
							console.log('----- 调用fetchdata err ------')
							cbb(er)
						}
						cbb(null)
					})
				},function(err){
					if(err){
						console.log('----------------------------- eachLimit final err -----------------------------')
						console.log(err)
						//return callback(err)
					}
					//console.time('time')
					console.log('----------------------------- each code arr end -----------------------------')
					console.timeEnd('fetchData costs time ------------------------------->')
					cb(null)
				})
			}); 
		}
	],function(error,result){
		if(error){
			console.log('-------------------------- async err ----------------------------')
			console.log(error)
		}
		console.log('------------------------------ async done -------------------------------')
		console.log('zongshu-->',zongshu)
	})
	console.timeEnd('process costs time ----------------------------->')
}

exports.rongzi = function(callback){
	console.log('rongzi function')
	let res_arr = new Array(),
		temp_obj = {}
	async.waterfall([
		function(cb){
			console.time('alltime')
			//数据来源
			//http://data.10jqka.com.cn/market/rzrqgg/code/601318/order/desc/page/1/ajax/1/
			let url = 'http://data.10jqka.com.cn/market/rzrqgg/code/601318/'
			superagent
				.get(url)
				.charset('gbk')
				.end(function(err,res){
					if(err){
						console.log('----- 获取页数失败 -----')
						console.log('状态码-->',response.status)
						console.log(err)
						cbb(err)
					}
					if(res.status === 200){
						let $ = cheerio.load(res.text)
						//获取数据总页数
						let pagenum = $('.page_info').text().split('/')[1]
						console.log('页数总共有-->',pagenum)

						//构造数据来源链接
						let data_url_arr = new Array()
						for(let i=1;i<parseInt(pagenum)+1;i++){
							temp_url = domain + '/market/rzrqgg/code/601318/order/desc/page/' + i + '/ajax/1'
							data_url_arr.push(temp_url) 
						}
						//console.log('待爬取链接-->',data_url_arr)
						cb(null,data_url_arr)
					}
				})
		},
		function(arg,cb){
			console.time('fetchData')
			async.eachLimit(arg,10,function(item,cbb){
				console.log('item-->',item)
				superagent
					.get(item)
					.charset('gbk')
					.end(function(err,response){
						if(err){
							console.log('----- 抓取时错误 -----')
							console.log('状态码-->',response.status)
							console.log(err)
							cb(err)
						}
						if(response.status === 200){
							let $ = cheerio.load(response.text)
							//获取所有的 tbody tr
							let tbody_tr = $('tbody').find('tr'),
								tbody_tr_length = tbody_tr.length
							console.log('本页记录条数-->',tbody_tr_length)

							let tbody_tr_td = $('tbody').find('tr').children('td'),
								tbody_tr_td_length = tbody_tr_td.length
							console.log('总的td数-->',tbody_tr_td_length)
							console.log('--------------------------- what happen ---------------------------')
							//let res_arr = new Array(),
							//	temp_obj = {}
							tbody_tr_td.each(function(i,it){
								switch(i%11){
									case 0:
										//console.log(it)
										temp_obj.xh = it.children[0].data
										break
									case 1:
										temp_obj.jysj = it.children[0].data.trim()
										break
									case 2:
										temp_obj.rzye = formatNum(it.children[0].data)
										break
									case 3:
										temp_obj.rzmr = formatNum(it.children[0].data)
										break
									case 4:
										temp_obj.rzch = formatNum(it.children[0].data)
										break
									case 5:
										temp_obj.rzjmr = formatNum(it.children[0].data)
										break
									case 6:
										temp_obj.rqyl = it.children[0].data
										break
									case 7:
										temp_obj.rqmc = it.children[0].data
										break
									case 8:
										temp_obj.rqch = it.children[0].data
										break
									case 9:
										temp_obj.rqjmc = it.children[0].data
										break
									case 10:
										temp_obj.rzrqye = formatNum(it.children[0].data)
										res_arr.push(temp_obj)
										temp_obj = {}
										break
								}
							})
							console.log('--------------------------- what happen ---------------------------')
							cbb(null)
						}
					})
			},function(error){
				if(error){
					console.log('----- async error -----')
					console.log(error)
					cb(error)
				}
				console.timeEnd('fetchData')
				cb(null,res_arr)
			})
		},
		function(arg,cb){
			console.time('search&remove')
			let search = rzrq.find({code:'601318'})
				search.exec(function(err,docs){
					if(err){
						console.log('----- search err -----')
						console.log(err)
						cb(err)
					}
					rzrq.remove({code:'601318'},function(er,doc){
						if(er){
							console.log('----- remove records err -----')
							console.log(er)
							cb(er)
						}
						console.log('----- remove success -----')
						console.log('code -->','601318')
						console.timeEnd('search&remove')
						cb(null,arg)
					})
				})
		},
		function(arg,cb){
			console.time('insert')
			console.log('数据记录数-->',arg.length)
			async.eachLimit(arg,1,function(item,cbbb){
				let insert_rzrq = new rzrq({
					code : '601318',
					xh : item.xh,
					jysj : item.jysj,
					rzye : item.rzye,
					rzmr : item.rzmr,
					rzch : item.rzch,
					rzjmr : item.rzjmr,
					rqyl : item.rqyl,
					rqmc : item.rqmc,
					rqch : item.rqch,
					rqjmc : item.rqjmc,
					rzrqye : item.rzrqye
				})
				insert_rzrq.save(function(err){
					if(err){
						console.log('----- save err -----')
						console.log(err)
						cbbb(err)
					}
					cbbb(null)
				})
			},function(err){
				if(err){
					console.log('----- eachLimit save err -----')
					cb(err)
				}
				console.timeEnd('insert')
				cb(null,arg)
			})
		}
	],function(error,result){
		console.timeEnd('alltime')
		if(error){
			console.log('----- async waterfall error -----')
			console.log(error)
			return callback(error)
		}
		//console.log('爬取结果-->',result)
		return callback(result)
	})
}

//格式化数据
function formatNum(num){
	let tem1 = '',
		tem2 =  ''

	if(num){
		tem1 = num.split('亿')
		tem2 = num.split('万')
	}

	if(tem1.length > 1){
		return tem1[0]
	}
	if(tem2.length > 1){
		tem2 = ((tem2[0])/10000).toFixed(6)
		return tem2
	}
}

//获取rzrq数据
exports.getRzrqData = function(callback){
	let search = rzrq.find({}).distinct('code')
		search.exec(function(err,docs){
			if(err){
				console.log('--------------------- search err -----------------------')
				console.log(err)
				callback(err)
			}
			console.log('---------------------- check docs -------------------------')
			console.log(docs)
			callback(null,docs)
		})
}