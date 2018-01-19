'use strict';

const nodemailer = require('nodemailer');

/**
 * Send an email through SMTP
 * @param subject
 * @param text
 * @param callback
 */
module.exports.send = function (subject, text, callback = function(){}) {
    const from = 'cann0neer.test@gmail.com';
    const to   = 'david.vonka@gmail.com';

    let smtpConfig = {
        host: 'smtp.gmail.com',
        port: 465,
        secure: true, // use SSL
        auth: {
            user: 'cann0neer.test@gmail.com',
            pass: 'some-pass-word'
        }
    };
    let transporter = nodemailer.createTransport(smtpConfig);

    let options = {
        from: from,
        replyTo: '"Aleks Kolyadenko" <kolyadenko.aleks@gmail.com>',
        to: to, // list of receivers
        subject: subject,
        text: text
    };

    transporter.sendMail(options, callback);
};