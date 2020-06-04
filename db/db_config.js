const isDocker = require('is-docker');
require('dotenv').config('./../');

const config = {
	// Match with the db container configuration
	host: isDocker()
		? process.env.DOCKER_DATABASE_HOST
		: 'localhost',
	port: 3306,
	user: 'root',
	password: 'root',
	database: 'tesdb'
};

module.exports = config;