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
 *
 * 4) The test assignment suggests to have installation/execution simple.
 * The service uses free remote DB 'Mongodb Atlas'.
 * In this case, any installation are not required.
 *
 * 5) It's expensive to make a query to DB every time when a new message is received.
 * The service uses Bloom filter to minimize DB usage.
 */

const mailer      = require('./libs/mailer');
const Log         = require('./models/Log');
const amqp        = require('amqplib/callback_api');
const async       = require('async');
const bloomFilter = require('bloom-filter');
const mongoose    = require('mongoose');

// mongodb Atlas uri
const DBUri = 'mongodb://cann0neer:some-pass-word@cluster0-shard-00-00-uzyqt.mongodb.net:27017,cluster0-shard-00-01-uzyqt.mongodb.net:27017,cluster0-shard-00-02-uzyqt.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin';

// bloom filter settings
const numberofElements = 3;
const falsePositiveRate = 0.01;

mongoose.Promise  = global.Promise;

mongoose.connect(DBUri).then(() =>  {

    async.waterfall([

        (cb) => { // init bloom filter
            let data = {};

            Log.findPlain({}, (err, logs) => {
                if (err) {
                    return cb(err);
                }

                data.filter = bloomFilter.create(numberofElements, falsePositiveRate);

                logs.forEach((log) => {
                    data.filter.insert(log.uid);
                });

                cb(null, data);
            });
        },

        (data, cb) => { // connect to RabbitMQ server
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

            // if the message has a wrong format - do nothing

            if (!msgObj.uid || !msgObj.val) {
                return;
            }

            // if Bloom filter doesn't contain uid - send mail
            // if it does - try to find uid in DB (Bloom filter can give a wrong result)

            if (!data.filter.contains(msgObj.uid)) {
                send();
            } else {
                Log.findPlain({uid: msgObj.uid}, (err, logs, total) => {
                    if (err) {
                        console.error(err);
                        return;
                    }

                    if (!total) {
                        send();
                    } else {
                        console.log('Duplicated message was received:', msgObj.val);
                    }
                });
            }

            function send () {
                console.log(` [x] Received "${msgObj.val}"`);

                data.filter.insert(msgObj.uid);

                Log.add({uid: msgObj.uid}, (err) => {
                    if (err) {
                        console.error(err);
                    }
                });

                mailer.send('Hi !', msgObj.val, (err, result) => {
                    if (err) {
                        console.error(err);
                        return;
                    }

                    console.log('Response:', result);
                });
            }
        }, {noAck: true});
    });
})
.catch((err) => console.error(err));
