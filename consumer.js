const amqp = require('amqplib/callback_api');
const isDocker = require('is-docker');
require('dotenv').config();

// Match with the rabbitmq container configuration
const AMQP_HOST = isDocker()
    ? process.env.DOCKER_RABBITMQ_HOST
    : 'localhost';
const AMQP_URL = `amqp://${AMQP_HOST}`;

const pool = require('./db/db_connection');

const CREATE_STUDENT_QUEUE = 'create-student';
const FIND_STUDENT_BY_ID_QUEUE = 'find-student-by-id';

let ch = null;
amqp.connect(AMQP_URL, (error, connection) => {
    if (error) {
        throw error;
    }

    connection.createChannel((error, channel) => {
        if (error) {
            throw error;
        }
        ch = channel;

        ch.assertQueue(CREATE_STUDENT_QUEUE, {
            durable: true
        }, (error, req_queue) => {
            if (error) {
                throw error;
            }

            ch.consume(req_queue.queue, (msg) => {
                try {
                    const obj = JSON.parse(msg.content);
                    pool.getConnection()
                        .then((db_connection) => {
                            db_connection.query(`INSERT INTO student(name) VALUES('${obj.payload}')`)
                                .then((results) => {
                                    ch.assertQueue(msg.properties.replyTo, {
                                        durable: true
                                    }, (error, res_queue) => {
                                        if (error) {
                                            throw error;
                                        }
                                        const reply = JSON.stringify(results);
                                        ch.sendToQueue(res_queue.queue, Buffer.from(reply), {
                                            correlationId: msg.properties.correlationId
                                        });
                                    });
                                })
                                .catch((err) => {
                                    ch.assertQueue(msg.properties.replyTo, {
                                        durable: true
                                    }, (error, res_queue) => {
                                        if (error) {
                                            throw error;
                                        }
                                        const reply = JSON.stringify(err);
                                        ch.sendToQueue(res_queue.queue, Buffer.from(reply), {
                                            correlationId: msg.properties.correlationId
                                        });
                                    });
                                })
                                .finally(() => {
                                    db_connection.release();
                                });
                        })
                        .catch((error) => {
                            throw error;
                        });
                } catch (err) {
                    ch.assertQueue(msg.properties.replyTo, {
                        durable: true
                    }, (error, res_queue) => {
                        if (error) {
                            throw error;
                        }
                        const reply = JSON.stringify(err);
                        ch.sendToQueue(res_queue.queue, Buffer.from(reply), {
                            correlationId: msg.properties.correlationId
                        });
                    });
                }
            });
        });

        ch.assertQueue(FIND_STUDENT_BY_ID_QUEUE, {
            durable: true
        }, (error, req_queue) => {
            if (error) {
                throw error;
            }

            ch.consume(req_queue.queue, (msg) => {
                try {
                    const obj = JSON.parse(msg.content);
                    pool.getConnection()
                        .then((db_connection) => {
                            db_connection.query(`SELECT * FROM student WHERE registration_number = ${obj.payload}`)
                                .then((results) => {
                                    ch.assertQueue(msg.properties.replyTo, {
                                        durable: true
                                    }, (error, res_queue) => {
                                        if (error) {
                                            throw error;
                                        }
                                        const reply = JSON.stringify(results[0]);
                                        ch.sendToQueue(res_queue.queue, Buffer.from(reply), {
                                            correlationId: msg.properties.correlationId
                                        });
                                    });
                                })
                                .catch((err) => {
                                    ch.assertQueue(msg.properties.replyTo, {
                                        durable: true
                                    }, (error, res_queue) => {
                                        if (error) {
                                            throw error;
                                        }
                                        const reply = JSON.stringify(err);
                                        ch.sendToQueue(res_queue.queue, Buffer.from(reply), {
                                            correlationId: msg.properties.correlationId
                                        });
                                    });
                                })
                                .finally(() => {
                                    db_connection.release();
                                });
                        })
                        .catch((error) => {
                            throw error;
                        });
                } catch (err) {
                    ch.assertQueue(msg.properties.replyTo, {
                        durable: true
                    }, (error, res_queue) => {
                        if (error) {
                            throw error;
                        }
                        const reply = JSON.stringify(err);
                        ch.sendToQueue(res_queue.queue, Buffer.from(reply), {
                            correlationId: msg.properties.correlationId
                        });
                    });
                }
            });
        });
    });
});