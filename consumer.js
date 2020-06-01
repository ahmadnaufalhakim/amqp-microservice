const amqp = require('amqplib/callback_api');
const AMQP_URL = 'amqp://localhost';

const pool = require('./db/db_connection');

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

        ch.consume('create-student', (msg) => {
            try {
                const obj = JSON.parse(msg.content);
                pool.query(`INSERT INTO student(name) VALUES('${obj.payload}')`)
                    .then((results) => {
                        console.log(results);
                        ch.sendToQueue('create-student_res', Buffer.from(JSON.stringify(results)), { persistent: true });
                    })
                    .catch((error) => {
                        console.log(error);
                        ch.sendToQueue('create-student_res', Buffer.from(JSON.stringify(error)), { persistent: true });
                    });
            } catch (error) {
                console.log(error);
                ch.sendToQueue('create-student_res', Buffer.from(JSON.stringify(error), { persistent: true }));
            }
        }, { noAck: true });

        ch.consume('find-student-by-id', (msg) => {
            try {
                const obj = JSON.parse(msg.content);
                pool.query(`SELECT * FROM student WHERE registration_number = ${obj.payload}`)
                    .then((results) => {
                        console.log(results[0]);
                        ch.sendToQueue('find-student-by-id_res', Buffer.from(JSON.stringify(results[0])), { persistent: true });
                    })
                    .catch((error) => {
                        console.log(error);
                        ch.sendToQueue('find-student-by-id_res', Buffer.from(JSON.stringify(error)), { persistent: true });
                    });
            } catch (error) {
                console.log(error);
                ch.sendToQueue('find-student-by-id_res', Buffer.from(JSON.stringify(error)), { persistent: true });
            }
        }, { noAck: true });
    });
});

process.on('exit', () => {
    ch.close();
    console.log('Closing RabbitMQ channel');
});