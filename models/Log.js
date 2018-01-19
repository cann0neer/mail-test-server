'use strict';

/**
 * Model for storing logs
 * (uids's of every message)
 * @type {*|Mongoose}
 */

const mongoose = require('mongoose');
const extend   = require('util')._extend;

let schema = new mongoose.Schema({
	uid    : {type: String, unique: true}
});

let Log = mongoose.model('logs', schema);
exports.model = Log;


/**
 * Создание нового документа
 * @param obj
 * @param callback
 */
exports.add = function (obj, callback) {
	let log = new Log(obj);
	log.save(callback);
};

/**
 * Удаление документа
 * @param query
 * @param callback
 */
exports.remove = function (query, callback) {
	Log.remove(query, callback);
};


/**
 * Ищет и отдает плоские объекты документов
 * @param findObj
 * @param params
 * @param callback
 */
exports.findPlain = function (findObj, params, callback) {
	if (typeof params === 'function') {
		callback = params;
		params = {};
	}

	params = extend({skip: 0, limit: 100, sort: {ts: -1}, select: {}}, params);

    Log.find(findObj)
		.limit(params.limit)
		.skip(params.skip)
		.sort(params.sort)
		.select(params.select)
		.lean()
		.exec(function (err, rows) {
			if (!err) {
                Log.count(findObj).exec(function (err, total) {
					callback(err, rows, total);
				});
			} else {
				callback(err, rows);
			}
		});
};
