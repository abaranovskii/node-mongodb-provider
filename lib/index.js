/**
 * Module dependencies.
 */

var utils = require('./utils')
    , tick = utils.tick;

/**
 * Базовый класс провайдера, от него должны унаследоваться все провайдеры
 * @param {Object} db - объект подключения к базе данных
 * @api public
 * @constructor
 */
function AbstractProvider(db) {
    this.db = db;

    // Проверяем наличие нужных индексов
    this._checkIndexExisting();
}

/**
 * Объект соединения с базой данных
 * @type {String}
 * @default null
 */
AbstractProvider.prototype.db = null;

/**
 * Название колекции в которой будут храниться документыы
 * @type {String}
 * @default null
 */
AbstractProvider.prototype.collection = null;


/**
 * Метод оберкта служит дл получения коллекции в строгом(безопасном) режиме
 * @param {Function} callback
 * @private
 */
AbstractProvider.prototype._getCollection = function (callback) {

    // Получаем колекцию в строгом режиме
    this.db.collection(this.collection, {w: 1, strict: true}, tick(callback));
};

/**
 * Проверяет наличие нужных индексов. Переопределяется производными классами
 * @private
 */
AbstractProvider.prototype._checkIndexExisting = function () {/** Must overriden by subclasses */
};


/**
 * Получает массив документов по переданным условиям поиска
 * @param {Object} conditions - условия поиска
 * @param {Object} projection - поля документа, которые будут возвращены
 * @param {Object} options - параметры поиска (сортировка, пагинация и т.д)
 * @param {Function} callback
 * @api public
 */
AbstractProvider.prototype.find = function (conditions, projection, options, callback) {
    if (typeof projection === 'function') {
        callback = projection;
        projection = {};
        options = {};
    }

    if (typeof options === 'function') {
        callback = options;
        options = {};
    }

    return this._getCollection(function (err, collection) {
        if (err) return callback(err);

        // Получаем массив документов по переданным условиям
        return collection.find(conditions, projection, options).toArray(tick(function (err, result) {
            if (err) return callback(err);

            return callback(null, result);
        }));
    });
};

/**
 * Получает один документ по переданным условиям поиска, если документ не найден, то возвращает ошибку
 * @param {Object} conditions - условия поиска
 * @param {Object} projection - поля документа, которые будут возвращены
 * @param {Function} callback
 * @api public
 */
AbstractProvider.prototype.findOne = function (conditions, projection, callback) {
    if (typeof projection === "function") {
        callback = projection;
        projection = {};
    }

    return this._getCollection(function (err, collection) {
        if (err) return callback(err);

        // Получаем один документ по переданным условием поиска
        return collection.findOne(conditions, projection, tick(function (err, doc) {
            if (err) return callback(err);
            if (!doc) return callback(new Error('Не удалось найти документ: ' + JSON.stringify(conditions)));

            return callback(null, doc);
        }));
    });
};

/**
 * Получает один документ по переданным условиям поиска, если документ не найден, то ошибку не возвращает
 * @param {Object} conditions - условия поиска
 * @param {Object} projection - поля документа, которые будут возвращены
 * @param {Function} callback
 * @api public
 */
AbstractProvider.prototype.findOnly = function (conditions, projection, callback) {
    if (typeof projection === "function") {
        callback = projection;
        projection = {};
    }

    return this._getCollection(function (err, collection) {
        if (err) return callback(err);

        // Получаем один документ по переданным условием поиска
        return collection.findOne(conditions, projection, tick(function (err, doc) {
            if (err) return callback(err);
            return callback(null, doc);
        }));
    });
};

/**
 * Отправляет запрос aggregate
 * @param {Array} pipeline - последовательность операций или стратегий
 * @param {Object} options - параметры
 * @param {function} callback
 */
AbstractProvider.prototype.aggregate = function (pipeline, options, callback) {
    if (typeof options === "function") {
        callback = options;
        options = {};
    }

    return this._getCollection(function (err, collection) {
        if (err) return callback(err);

        // Отправляем запрос
        return collection.aggregate(pipeline, options, callback);
    });
};

/**
 * Создаем массив документов
 * @param {Array} docs - массив документов
 * @param {Function} callback
 * @api public
 */
AbstractProvider.prototype.create = function (docs, callback) {

    return this._getCollection(function (err, collection) {
        if (err) return callback(err);

        // Создаем новую коллекцию с полученикм результата
        return collection.insert(docs, {w: 1}, tick(function (err, result) {
            if (err) return callback(err);
            if (!result || !result.length) return callback(new Error('Не удалось создать документы: ' + JSON.stringify(docs)));

            return callback(null, result);
        }));
    });
};

/**
 * Создает один документ
 * @param {Object} doc - создаваемый объект
 * @param {Function} callback
 */
AbstractProvider.prototype.createOne = function (doc, callback) {

    return this._getCollection(function (err, collection) {
        if (err) return callback(err);

        // Создаем новый документ
        return collection.insert(doc, {w: 1}, tick(function (err, result) {
            if (err) return callback(err);
            if (!result || !result.length) return callback(new Error('Не удалось создать документ: ' + JSON.stringify(doc)));

            // Возвращает первый документ
            return callback(null, result[0]);
        }));
    });

};

/**
 * Обновляет документы, удовлетворяющие conditions
 * @param {Object} conditions - условия поиска
 * @param {Object} update - документ обновления
 * @param {Function} callback
 */
AbstractProvider.prototype.update = function (conditions, update, callback) {

    return this._getCollection(function (err, collection) {
        if (err) return callback(err);

        return collection.update(conditions, update, {w: 1, multi: true}, tick(function (err, numAffected) {
            if (err) return callback(err);
            if (!numAffected) return callback(new Error('Не удалось обновить документы: ' + JSON.stringify(conditions)));

            return callback(null);
        }));
    });
};

/**
 * Обновляет один документ удовлетворяющий conditions
 * @param {Object} conditions - условия поиска
 * @param {Object} update - объект обновленя
 * @param {Object} [options] - конфиг
 * @param {Function} callback
 */
AbstractProvider.prototype.updateOne = function (conditions, update, options, callback) {
    if (typeof options === "function") {
        callback = options;
        options = {};
    }

    return this._getCollection(function (err, collection) {
        if (err) return callback(err);

        // Обновляем документы
        return collection.update(conditions, update, utils.extend({w: 1}, options), tick(function (err, numAffected) {
            if (err) return callback(err);
            if (!numAffected) return callback(new Error('Не удалось обновить документ: ' + JSON.stringify(conditions)));

            return callback(null);
        }));
    });
};

/**
 * Модифицирует и возвращает (один) обновленный документ
 * @param {Object} conditions - условия поиска
 * @param {Object} update - объект обновления
 * @param {Function} callback
 */
AbstractProvider.prototype.findAndUpdate = function (conditions, update, callback) {

    return this._getCollection(function (err, collection) {
        if (err) return callback(err);

        // Обновляем документ и возвращаем обновленный (new: true)
        return collection.findAndModify(conditions, [], update, {new: true, w: 1}, tick(function (err, doc) {
            if (err) return callback(err);
            if (!doc) return callback(new Error('Не удалось найти и обновить документ: ' + JSON.stringify(conditions)));

            return callback(null, doc);
        }));
    });
};

/**
 * Удаляет документы
 * @param {Object} conditions - условия поиска
 * @param {Function} callback
 */
AbstractProvider.prototype.remove = function (conditions, callback) {

    return this._getCollection(function (err, collection) {
        if (err) return callback(err);

        // Удаляем документы
        return collection.remove(conditions, {w: 1}, tick(function (err, numRemoved) {
            if (err) return callback(err);
            if (!numRemoved) return callback(new Error('Не удалось удалить документы: ' + JSON.stringify(conditions)));

            return callback(null);
        }));
    });
};

/**
 * Удаляет один документ, если не удалил, то ошибку не возвращает
 * @param {Object} conditions - условия поиска
 * @param {Function} callback
 */
AbstractProvider.prototype.removeOnly = function (conditions, callback) {

    return this._getCollection(function (err, collection) {
        if (err) return callback(err);

        // Удаляем документы
        return collection.remove(conditions, {w: 1}, tick(function (err, numRemoved) {
            if (err) return callback(err);
            return callback(null, numRemoved);
        }));
    });
};

/**
 * Удаляет и возвращает (один) удаленный документ
 * @param {Object} conditions - условия поиска
 * @param {Function} callback
 */
AbstractProvider.prototype.findAndRemove = function (conditions, callback) {

    return this._getCollection(function (err, collection) {
        if (err) return callback(err);

        // Удаляем и возвращаем удаленный документ
        return collection.findAndRemove(conditions, [], {w: 1}, function (err, doc) {
            if (err) return callback(err);
            if (!doc) return callback(new Error('Не удалось найти и удалить документ:' + JSON.stringify(conditions)));

            return callback(null, doc);
        });
    });
};

/**
 * Возвращает количество документов, удовлетворяющих условиям поиска
 * @param {Object} conditions - условия поиска
 * @param {Function} callback
 */
AbstractProvider.prototype.count = function (conditions, callback) {

    return this._getCollection(function (err, collection) {
        if (err) return callback(err);

        return collection.count(conditions, function (err, count) {
            if (err) return callback(err);

            return callback(null, count);
        });
    });
};

// Экспортируем конструктор провайдера
module.exports = AbstractProvider;
