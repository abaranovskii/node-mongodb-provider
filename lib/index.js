/**
 * Module dependencies.
 */


/**
 * Базовый класс провайдера, от него должны унаследоваться все провайдеры
 * @param {Object} db - объект подключения к базе данных
 * @api public
 * @constructor
 */
function AbstractProvider(db) {
  this.db = db;

  // Устанавливаем индексы (если есть)
  //this.ensureIndexes();
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
 * Индексы, которые будут устанавливаться при инициализации провайдера
 * @see http://docs.mongodb.org/manual/core/indexes/
 * @type {null}
 * @default null
 */
AbstractProvider.prototype.indexes = null;










/**
 * Метод оберкта служит дл получения коллекции в строгом(безопасном) режиме
 * @param {Function} callback
 */
AbstractProvider.prototype._getCollection = function(callback) {

  // Получаем колекцию в строгом режиме
  this.db.collection(this.collection, {w: 1}, callback);
};


/**
 * Создает индекст по полям коллекции, если еще не созданы
 * @api public
 */
/*AbstractProvider.prototype.ensureIndexes = function () {
  var indexes = Array.isArray(this._indexes) ? this._indexes : [];
  if (!indexes.length) {
    //TODO: Какой тип вызова calback лучше всего использовать?
    //return cb && process.nextTick(cb);
    return
  }

  //TODO: Сейчас индексы создаются по одному, посмотреть можно ли их создать в batch?

  return this._getCollection(function (err, collection) {
    if (err) return console.log(err);

    function create () {
      var index = indexes.shift();
      if (!index) return console.log(err);

      // TODO: Нужно ли здесь использовать process.nextTick?
      return collection.ensureIndex(index[0], index[1], function (err) {
        if (err) return console.log(err);

        return create();
      });
    }

    return create();
  });
};*/


/**
 * Получает массив документов по переданным условиям поиска
 * @param {Object} conditions - условия поиска
 * @param {Function} callback
 * @api public
 */
AbstractProvider.prototype.find = function (conditions, callback) {

  return this._getCollection(function (err, collection) {
    if (err) return callback(err);

    // Получаем массив документов по переданным условиям
    return collection.find(conditions).toArray(function (err, result) {
      if (err) return callback(err);

      return callback(null, result);
    });
  });
};


/**
 * Получает один документ по переданным условиям поиска
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
    return collection.findOne(conditions, projection, function (err, doc) {
      if (err) return callback(err);
      if (!doc) return callback(new Error('Не удалось найти документ: ' + conditions));

      return callback(null, doc);
    });
  });
};


/**
 * Создаем массив документов
 * @param {Array} docs
 * @param {Function} callback
 * @api public
 */
AbstractProvider.prototype.create = function (docs, callback) {

  return this._getCollection(function (err, collection) {
    if (err) return callback(err);

    // Создаем новую коллекцию с полученикм результата
    return collection.insert(docs, {w: 1}, function (err, result) {
      if (err) return callback(err);
      if (!result || !result.length) return callback(new Error('Не удалось создать документы: ' + docs));

      return callback(null, result);
    });
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
    return collection.insert(doc, {w:1}, function (err, result) {
      if (err) return callback(err);
      if (!result || !result.length) return callback(new Error('Не удалось создать документ: ' + doc));

      // Возвращает первый документ
      return callback(null, result[0]);
    });
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

    return collection.update(conditions, update, {w: 1, multi: true}, function (err) {
      if (err) return callback(err);

      return callback(null);
    });
  });
};

/**
 * Обновляет один документ удовлетворяющий conditions
 * @param {Object} conditions - условия поиска
 * @param {Object} update - объект обновленя
 * @param {Function} callback
 */
AbstractProvider.prototype.updateOne = function (conditions, update, callback) {

  return this._getCollection(function (err, collection) {
    if (err) return callback(err);

    // Обновляем документы
    return collection.update(conditions, update, {w:1}, function (err, numAffected) {
      if (err) return callback(err);
      if (!numAffected) return callback(new Error('Не удалось обновить документ: ' + conditions));

      return callback(null);
    });
  });
};




/**
 * Модифицирует и возвращает (один) обновленный документ
 * @param {Object} conditions - условия поиска
 * @param {Object} update - объект обновления
 * @param {Function} callback
 */
AbstractProvider.prototype.findAndUpdate = function(conditions, update, callback) {

  return this._getCollection(function (err, collection) {
    if (err) return callback(err);

    // Обновляем документ и возвращаем обновленный (new: true)
    return collection.findAndModify(conditions, [], update, {new: true}, function (err, doc) {
      if (err) return callback(err);
      if (!doc) return callback(new Error('Не удалось найди документ: ' + conditions));

      return callback(null, doc);
    });
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
    return collection.remove(conditions, {w:1}, callback);
  });
};


// Экспортируем конструктор провайдера
module.exports = AbstractProvider;