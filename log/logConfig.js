var log4js = require('log4js');

var log4js_config = require("../log4js.json");
log4js.configure(log4js_config);

var logger = log4js.getLogger('');
var consoleLog = log4js.getLogger('');

//exports.logger = consoleLog;
exports.logger = logger

