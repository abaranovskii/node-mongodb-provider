/*!
 * Module dependencies.
 */

var ObjectID = require('mongodb').ObjectID;
/*!
 * process.nextTick helper.
 *
 * Wraps `callback` in a try/catch + nextTick.
 *
 * node-mongodb-native has a habit of state corruption when an error is immediately thrown from within a collection callback.
 *
 * @param {Function} callback
 * @api private
 */

exports.tick = function tick (callback) {
  if ('function' !== typeof callback) return;
  return function () {
    try {
      callback.apply(this, arguments);
    } catch (err) {
      // only nextTick on err to get out of
      // the event loop and avoid state corruption.
      process.nextTick(function () {
        throw err;
      });
    }
  }
};


/**
 * Приводит переданный тип данных к объекту ObjectId
 * @param {String|Array} data - данные для приведения к ObjectId
 */
exports.toObjectID = function (data) {
  if (Array.isArray(data)) {
    for (var i=0, len=data.length; i<len; i++) {
      data[i] = (data[i] instanceof ObjectID) ? data[i] : ObjectID(data[i]);
    }
  } else {
    data = (data instanceof ObjectID) ? data : ObjectID(data);
  }

  return data;
};