const charset = require('superagent-charset')
const superagent = charset(require('superagent'))
const request = require('request')
const cheerio =require('cheerio')
const async = require('async')
const schedule = require("node-schedule")
const iconv = require('iconv-lite')
iconv.skipDecodeWarning = true;
const logger = require('../log/logConfig').logger

const rzrq = require('../db/rzrq')

var zongshu = 0,
	fail_url_arr = [],
	fail_url_arr_sec = []

const domain = 'http://data.10jqka.com.cn' //域名
//const code_url = 'http://data.10jqka.com.cn/market/rzrq/' //获取code链接

//获取所有的code(rongzi)
function fetchCodeArr(callback){
	console.time('获取code总耗时---->')
	let code_arr = [],
		temp_obj = {}
	async.waterfall([
		function(cb){
			let get_code_url = domain + '/market/rzrq/'
			logger.info('获取code页面链接-->',get_code_url)

			request({url:get_code_url, timeout:30000},function(err,response,body){
				if(err){
					logger.error('---------------------- get_code_url err ----------------------')
					logger.error(err)
					logger.error(err.code === 'ETIMEDOUT');
					    // Set to `true` if the timeout was a connection timeout, `false` or
					    // `undefined` otherwise.
					    logger.error(err.connect === true);
					cb(err)
				}
				if (!err && response.statusCode == 200) {
				    //logger.info(body) // Show the HTML for the baidu homepage.
				    let str = iconv.decode(body, 'GBK')
				    let $ = cheerio.load(str)
				    //获取页面数
					let pagenum = $('.page_info').text().split('/')[2]
					logger.info($('.page_info').text())
					logger.info('code 页面数-->',pagenum)
					//http://data.10jqka.com.cn/market/rzrq/board/ls/field/rzjmr/order/desc/page/19/ajax/1
					//获取code的链接 http://data.10jqka.com.cn/market/rzrq/board/ls/field/rzjmr/order/desc/page/3/ajax/1/
					let code_url_arr = []
					for(let i=1;i<=parseInt(pagenum);i++){
						temp_url = domain + '/market/rzrq/board/ls/field/rzjmr/order/desc/page/' + i + '/ajax/1'
						code_url_arr.push(temp_url)
					}
					logger.info('待爬取链接-->',code_url_arr)
					cb(null,code_url_arr)
				 }
			})
		},
		function(code_url_arr,cb){
			async.eachLimit(code_url_arr,1,function(item,cbb){
				logger.info('当前爬取链接-->',item)
				
				request({url:item, timeout:30000},function(err,response,body){
					//logger.info(err)
					//logger.info(response.status)
					//logger.info(body)
					if(err){
						logger.error('---------------------- get_code_url err ----------------------')
						logger.error(err)
						logger.error(err.code === 'ETIMEDOUT');
					    // Set to `true` if the timeout was a connection timeout, `false` or
					    // `undefined` otherwise.
					    logger.error(err.connect === true);
						cbb(err)
					}
					if(!err && response.statusCode == 200){
						let str = iconv.decode(body, 'GBK')
							let $ = cheerio.load(str)
							//获取所有的 tbody tr
							let tbody_tr = $('tbody').find('tr'),
								tbody_tr_length = tbody_tr.length
							logger.info('本页记录条数-->',tbody_tr_length)

							let tbody_tr_td = $('tbody').find('tr').children('td'),
								tbody_tr_td_length = tbody_tr_td.length
							logger.info('总的td数-->',tbody_tr_td_length)
							logger.info('--------------------------- what happen ---------------------------')
							tbody_tr_td.each(function(i,it){
								let temp_i = i%13
								switch(temp_i){
									case 0:
										//logger.info(it)
										temp_obj.xh = it.children[0].data
										break
									case 1:
										temp_obj.code = $(it).children()[0].children[0].data
										break
									case 2:
										temp_obj.name = $(it).children()[0].children[0].data
										break
									case 3:
										temp_obj.rzye = it.children[0].data
										break
									case 4:
										temp_obj.rzmr = it.children[0].data
										break
									case 5:
										temp_obj.rzch = it.children[0].data
										break
									case 6:
										temp_obj.rzjmr = it.children[0].data
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
										temp_obj.rzrqye = it.children[0].data
										break
									case 12:
										temp_obj.his = $(it).children()[0].children[0].data
										code_arr.push(temp_obj)
										temp_obj = {}
										break
								}
							})
							logger.info('--------------------------- what happen ---------------------------')
							logger.info('code_arr length ------------------------------>',code_arr.length)
							cbb()
						}

				})
			},function(err){
				if(err){
					logger.error('------------------------ eachLimit err --------------------------')
					logger.error(err)
					cb(err)
				}
				logger.info(code_arr)
				cb(null,code_arr)
			})
		}
	],function(error,result){
		if(error){
			logger.error('-------------------------- async err ---------------------------')
			logger.error(error)
			return callback(error)
		}
		console.timeEnd('获取code总耗时---->')
		logger.info('code 总数--->',result.length)
		return callback(null,result)
	})
}

function fetchData(code,callback){
	console.time('获取数据总耗时---->')
	let res_arr = new Array(),
		temp_obj = {}
	async.waterfall([
		function(cb){
			console.time('获取数据链接耗时---->')
			//数据来源
			//http://data.10jqka.com.cn/market/rzrqgg/code/601318/order/desc/page/1/ajax/1/
			//let url = 'http://data.10jqka.com.cn/market/rzrqgg/code/601318/'
			let url = domain + '/market/rzrqgg/code/' + code + '/'
			logger.info('获取页面链接-->',url)
			request({url:url, timeout:30000},function(err,response,body){
				if(err){
						logger.error('----- 获取页数失败 -----')
						//logger.info('状态码-->',response.status)
						logger.error(err)
						logger.error(err.code === 'ETIMEDOUT');
					    // Set to `true` if the timeout was a connection timeout, `false` or
					    // `undefined` otherwise.
					    logger.error(err.connect === true);
						cb(err)
					}
				if(!err && response.statusCode == 200){
					let str = iconv.decode(body, 'GBK')
						let $ = cheerio.load(str)
						//获取数据总页数
						let pagenum = $('.page_info').text().split('/')[1]
						logger.info('页数总共有-->',pagenum)

						//构造数据来源链接
						let data_url_arr = new Array()
						for(let i=1;i<parseInt(pagenum)+1;i++){
							//temp_url = domain + '/market/rzrqgg/code/601318/order/desc/page/' + i + '/ajax/1'
							temp_url = domain + '/market/rzrqgg/code/' + code +'/order/desc/page/' + i + '/ajax/1'
							data_url_arr.push(temp_url) 
						}
						logger.info('待爬取链接-->',data_url_arr)
						console.timeEnd('获取数据链接耗时---->')
						cb(null,data_url_arr)
				}
			})
		},
		function(arg,cb){
			console.time('获取数据耗时---->')
			//console.time('抓取数据耗时-->')
			async.eachLimit(arg,10,function(item,cbb){
				logger.info('当前爬取-->',item)//{url:item, timeout:30000}
				request({url:item, timeout:30000},function(err,response,body){
					if(err){
							logger.error('--------------------------- 抓取时错误 ----------------------------')
							//logger.info('状态码-->',response.statusCode)
							logger.error(err)
							fail_url_arr.push(item)
							logger.error(err.code === 'ETIMEDOUT');
						    // Set to `true` if the timeout was a connection timeout, `false` or
						    // `undefined` otherwise.
						    logger.error(err.connect === true);
							cbb(null)
							//cbb(err)
						}
						if(!err && response.statusCode == 200){
							let str = iconv.decode(body, 'GBK')
							let $ = cheerio.load(str)
							//获取所有的 tbody tr
							let tbody_tr = $('tbody').find('tr'),
								tbody_tr_length = tbody_tr.length
							logger.info('本页记录条数-->',tbody_tr_length)

							let tbody_tr_td = $('tbody').find('tr').children('td'),
								tbody_tr_td_length = tbody_tr_td.length
							logger.info('总的td数-->',tbody_tr_td_length)

							tbody_tr_td.each(function(i,it){
								let temp_i = i%11
								switch(temp_i){
									case 0:
										//logger.info(it)
										temp_obj.xh = it.children[0].data
										break
									case 1:
										temp_obj.jysj = it.children[0].data.trim()
										break
									case 2:
										temp_obj.rzye = it.children[0].data.trim()
										break
									case 3:
										temp_obj.rzmr = it.children[0].data.trim()
										break
									case 4:
										temp_obj.rzch = it.children[0].data.trim()
										break
									case 5:
										temp_obj.rzjmr = it.children[0].data.trim()
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
										temp_obj.rzrqye = it.children[0].data.trim()
										res_arr.push(temp_obj)
										temp_obj = {}
										break
								}
							})
							zongshu = zongshu + 1
							cbb(null)
						}
				})
			},function(error){
				if(error){
					logger.error('----- eachLimit error -----')
					logger.error(error)
					cb(error)
				}
				//console.timeEnd('抓取数据耗时-->')
				cb(null,res_arr)
			})
		},
		function(arg,cb){
			let search = rzrq.find({code:code})
				search.exec(function(err,docs){
					if(err){
						logger.error('----- search err -----')
						logger.error(err)
						cb(err)
					}
					rzrq.remove({code:code},function(er,doc){
						if(er){
							logger.error('----- remove records err -----')
							logger.error(er)
							cb(er)
						}
						logger.info('----- remove success -----')
						logger.info('对应代码 -->',code)
						cb(null,arg)
					})
				})
		},
		function(arg,cb){
			logger.info('数据记录数-->',arg.length)
			logger.info('count_fetch_url-->',count_fetch_url)
			async.eachLimit(arg,50,function(item,cbbb){
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
						logger.error('----- save err -----')
						logger.error(err)
						cbbb(err)
					}
					//logger.info('count_fetch_url-->save',count_fetch_url)
					cbbb(null)
				})
			},function(err){
				if(err){
					logger.error('----- eachLimit save err -----')
					cb(err)
				}
				logger.info('count_fetch_url-->eachLimit save',count_fetch_url)
				cb(null,arg)
			})
		}
	],function(error,result){
		if(error){
			logger.error('----- async waterfall error -----')
			logger.error(error)
			return callback(error)
		}
		//logger.info('爬取结果-->',result)
		//return callback(result)
		console.time('获取数据总耗时---->')
		return callback(null)
	})
}

/*作用与eachSeries类似，最后也只能收到err/null作为反馈，但与eachSeries串行执行不同，each是全部执行并等待全部完成后返回，中间如果有错误就不等全部完成就立即返回。
    可以看到，函数名中有each的是没有结果集的，函数明名map是有结果集的；
也可以看到,函数都是只要iterator返回err就立即调用callback返回。
并且其内部有控制过，callback只会被调用一次，如果你发现被回调了多次，那么这一定是一个bug，可以向作者反馈。*/
var count_fetch_url = 0
exports.execFetchData = function (){
	console.time('execFetchData costs time ----------------------------->')
	async.waterfall([
		function(cb){
			console.time('get code_arr costs time ------------------------>')
			fetchCodeArr(function(err,res){
				if(err){
					logger.error('-------------------------- 调用 fetchCodeArr err --------------------------')
					logger.error(err)
				}
				console.timeEnd('get code_arr costs time ------------------------>')
				cb(null,res)
			})
		},
		function(arg,cb){
			let date = new Date(2017,10,11,13,08,00);

			schedule.scheduleJob('41 08 * * *', function(){  //每天九点11分
				async.eachLimit(arg,10,function(item,cbb){
					//console.time('time')
					fetchData(item.code,function(er,re){
						if(er){
							logger.error('----- 调用fetchdata err ------')
							cbb(er)
						}
						count_fetch_url++
						cbb(null)
					})
				},function(err){
					if(err){
						logger.error('----------------------------- eachLimit final err -----------------------------')
						logger.error(err)
						//return callback(err)
					}
					//console.time('time')
					logger.info('----------------------------- each code arr end -----------------------------')
					cb(null)
				})
			}); 
		}
	],function(error,result){
		if(error){
			logger.error('-------------------------- async err ----------------------------')
			logger.error(error)
		}
		logger.info('------------------------------ async done -------------------------------')
		logger.info('count_fetch_url-->',count_fetch_url)
		logger.info('zongshu-->',zongshu)
		logger.info('第一次失败链接--->',fail_url_arr.length,fail_url_arr)
		//logger.info('最终失败链接--->',fail_url_arr_sec)
	})
	console.timeEnd('execFetchData costs time ----------------------------->')
}

exports.rongzi = function(callback){
	logger.info('rongzi function')
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
						logger.error('----- 获取页数失败 -----')
						logger.error('状态码-->',response.status)
						logger.error(err)
						cbb(err)
					}
					if(res.status === 200){
						let $ = cheerio.load(res.text)
						//获取数据总页数
						let pagenum = $('.page_info').text().split('/')[1]
						logger.debug('页数总共有-->',pagenum)

						//构造数据来源链接
						let data_url_arr = new Array()
						for(let i=1;i<parseInt(pagenum)+1;i++){
							temp_url = domain + '/market/rzrqgg/code/601318/order/desc/page/' + i + '/ajax/1'
							data_url_arr.push(temp_url) 
						}
						//logger.info('待爬取链接-->',data_url_arr)
						cb(null,data_url_arr)
					}
				})
		},
		function(arg,cb){
			console.time('fetchData')
			async.eachLimit(arg,10,function(item,cbb){
				logger.info('item-->',item)
				superagent
					.get(item)
					.charset('gbk')
					.end(function(err,response){
						if(err){
							logger.error('----- 抓取时错误 -----')
							logger.error('状态码-->',response.status)
							logger.error(err)
							cb(err)
						}
						if(response.status === 200){
							let $ = cheerio.load(response.text)
							//获取所有的 tbody tr
							let tbody_tr = $('tbody').find('tr'),
								tbody_tr_length = tbody_tr.length
							logger.info('本页记录条数-->',tbody_tr_length)

							let tbody_tr_td = $('tbody').find('tr').children('td'),
								tbody_tr_td_length = tbody_tr_td.length
							logger.debug('总的td数-->',tbody_tr_td_length)
							logger.debug('--------------------------- what happen ---------------------------')
							//let res_arr = new Array(),
							//	temp_obj = {}
							tbody_tr_td.each(function(i,it){
								switch(i%11){
									case 0:
										//logger.info(it)
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
							logger.debug('--------------------------- what happen ---------------------------')
							cbb(null)
						}
					})
			},function(error){
				if(error){
					logger.error('----- async error -----')
					logger.error(error)
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
						logger.error('----- search err -----')
						logger.error(err)
						cb(err)
					}
					rzrq.remove({code:'601318'},function(er,doc){
						if(er){
							logger.error('----- remove records err -----')
							logger.error(er)
							cb(er)
						}
						logger.info('----- remove success -----')
						logger.info('code -->','601318')
						console.timeEnd('search&remove')
						cb(null,arg)
					})
				})
		},
		function(arg,cb){
			console.time('insert')
			logger.info('数据记录数-->',arg.length)
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
						logger.error('----- save err -----')
						logger.error(err)
						cbbb(err)
					}
					cbbb(null)
				})
			},function(err){
				if(err){
					logger.error('----- eachLimit save err -----')
					cb(err)
				}
				console.timeEnd('insert')
				cb(null,arg)
			})
		}
	],function(error,result){
		console.timeEnd('alltime')
		if(error){
			logger.error('----- async waterfall error -----')
			logger.error(error)
			return callback(error)
		}
		//logger.info('爬取结果-->',result)
		return callback(result)
	})
}

//格式化数据
function formatNum(num){
	logger.info('num----->',num)
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
				logger.error('--------------------- search err -----------------------')
				logger.error(err)
				callback(err)
			}
			logger.info('---------------------- check docs -------------------------')
			logger.info(docs)
			callback(null,docs)
		})
}