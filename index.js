'use strict';

/**
 * A description of the implementation:
 *
 * 1) Type of RabbitMQ's exchange - 'direct'.
 * 'Durable' and 'persistent' flags are also used to avoid losing messages.
 *
 * 2) RabbitMQ has to guarantee sending messages without duplicates.
 * However, in some cases it's not true.
 * A quote from docs: "In the event of network failure (or a node crashing),
 * messages can be duplicated, and consumers must be prepared to handle them.
 * If possible, the simplest way to handle this is to ensure that your consumers handle messages in an idempotent way
 * rather than explicitly deal with deduplication."
 *
 * 3) Mongodb collection is used for deduplication.
 * The collection 'logs' stores unique ids of all sent messages.
 * A unique index on the 'uid' field prevents duplication.
 *
 * 4) The test assignment suggests to have installation/execution simple.
 * The service uses free remote DB 'Mongodb Atlas'.
 * In this case, any installation are not required.
 */

const mailer      = require('./libs/mailer');
const Log         = require('./models/Log');
const amqp        = require('amqplib/callback_api');
const async       = require('async');
const mongoose    = require('mongoose');

// mongodb Atlas uri
const DBUri = 'mongodb://cann0neer:some-pass-word@cluster0-shard-00-00-uzyqt.mongodb.net:27017,cluster0-shard-00-01-uzyqt.mongodb.net:27017,cluster0-shard-00-02-uzyqt.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin';

mongoose.Promise  = global.Promise;

mongoose.connect(DBUri).then(() =>  {

    async.waterfall([

        (cb) => { // connect to RabbitMQ server
            let data = {};

            amqp.connect('amqp://localhost', (err, conn) => {
                data.conn = conn;
                cb(err, data);
            });
        },

        (data, cb) => { // create channel
            data.conn.createChannel((err, channel) => {
                data.channel = channel;
                cb(err, data);
            });
        }
    ], (err, data) => {
        if (err) {
            console.error(err);
            process.exit(1); // no reason to continue
        }

        const q = 'hello';

        data.channel.assertQueue(q, {durable: true});

        console.log(` [*] Waiting for messages in ${q}. To exit press CTRL+C`);

        data.channel.consume(q, (msg) => {
            let msgStr = msg.content.toString();
            let msgObj;

            try {
                msgObj = JSON.parse(msgStr);
            } catch (e) {
                msgObj = {};
            }

            console.log(` [x] Received "${msgObj.val}" `, msgObj.uid);

            // if the message has a wrong format - do nothing

            if (!msgObj.uid || !msgObj.val) {
                return;
            }

            // the email will be sent only if uid is unique in collection logs

            Log.add({uid: msgObj.uid}, (err) => {
                if (err) {
                    if (err.code === 11000) {
                        console.log('Duplicated message was received:', msgObj.val);
                    } else {
                        console.error(err);
                    }

                    return;
                }

                mailer.send('Hi !', msgObj.val, (err, result) => {
                    if (err) {
                        console.error(err);
                        return;
                    }

                    console.log('Response:', result);
                });
            });
        }, {noAck: true});
    });
})
.catch((err) => console.error(err));
