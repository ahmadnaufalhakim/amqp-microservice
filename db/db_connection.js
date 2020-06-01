const mariadb = require('mariadb');
const config = require('./db_config');

const pool = mariadb.createPool(config);

module.exports = pool;