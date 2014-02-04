global.MONGOOSE_DRIVER_PATH = __dirname
module.exports = require('../lib')({nativeObjectID: false, searchInArray: true})
