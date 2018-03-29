/**
 * Module dependencies
 */

var _ = require('lodash');
var async = require('async');
var waterlineCriteria = require('waterline-criteria');
var Aggregate = require('./aggregates');
var Errors = require('waterline-errors').adapter;


/**
 * An Elasticsearch Datastore
 *
 * @param {Object} config
 * @param {Object} collections
 * @return {Object}
 * @api public
 */

var Database = module.exports = function(config, collections, cb) {
  var self = this;
  this.config = config || {};
  this.collections = collections || {};

  var elasticsearch = require('elasticsearch');
  var opts = {
    host: this.config.host,
    log: this.config.log
  }
  this.client = new elasticsearch.Client(opts);

  this.client.indices.exists({
    index: self.config.index
  }).then(function(exists) {
    var analysis = self.config.analysis || {};
    var mappings = self.config.mappings || {};

    if (!exists) {
      return self.client.indices.create({
        index: self.config.index,
        body: {
          "settings": {
            "number_of_shards": self.config.number_of_shards || 2,
            "number_of_replicas": self.config.number_of_replicas || 1,
            "analysis": analysis
          },
          "mappings": mappings
        }
    });
    } else {
      return self.client.indices.close({index: self.config.index})
      .then(function() {
        if (analysis) {
          return self.client.indices.putSettings({
            index: self.config.index,
            body: {analysis: analysis}
          })
        }
      })
      .then(function() {
        for (var type in mappings) {
          self.client.indices.putMapping({
            index: self.config.index,
            type: type,
            body: mappings[type]
          })
        }
      }).then(function() {
        return self.client.indices.open({index: self.config.index})
      })
    }
   }).catch(function(err) {
    throw new Error(err)
  });
  return this;
};


function _getQuery(query) {
  var _q;

  if (query.where && query.where.id) {
    _q = {query: {term: {id: query.where.id.toString()}}};
  } else {
    _q = query.where;
  }

  if (query.hasOwnProperty('limit') && query.limit) {
    _q.size = query.limit;
  }
      
  if (query.hasOwnProperty('skip') && query.skip) {
    _q.from = query.skip;
  }

  if (query.hasOwnProperty('sort') && query.sort) {
    if (Object.keys(query.sort).length > 0) {
      var sort = [];

      for (var key in query.sort) {
        var _tmp = {};
        if (key == "_script") {
          _tmp = {
            "_script": query.sort[key]
          };
        } else {
          _tmp[key] = {
            'order': query.sort[key] > 0 ? 'asc' : 'desc'
          };
        }

        sort.push(_tmp);
      }

      _q.sort = sort;
    }
  }
  return _q
}

/**
 * Query native response
 */
Database.prototype.query = function(collectionName, options, cb) {
  var searchObj = {
    index: this.config.index,
    body: _getQuery(options)
  };

  if (options.where && options.where.id) {
    // Get ONE
    this.client.get(searchObj, function(error, response) {
      cb(null, response);
    });

  } else {
    // Get Query

    this.client.search(searchObj, function(error, response) {
      if(!response || !response.hits || !response.hits.hits) return cb(error)
      cb(null, response);
    });
  }
};


/**
 * Select
 */
Database.prototype.select = function(collectionName, options, cb) {
  var searchObj = {
    index: this.config.index,
    body: _getQuery(options)
  };

  if (options.where && options.where.id) {
    // Get ONE
    this.client.get(searchObj, function(error, response) {
      cb(null, response._source);
    });

  } else {
    // Get Query

    this.client.search(searchObj, function(error, response) {
      if(!response || !response.hits || !response.hits.hits) return cb(error)
      var results = _.map(response.hits.hits, function(hit) {
        return Object.assign({_score: hit._score}, hit._source);
      });
      cb(null, results);
    });

  }
  // Filter Data based on Options criteria
  // var resultSet = waterlineCriteria(collectionName, this.data, options);
  // // Process Aggregate Options
  // var aggregate = new Aggregate(options, resultSet.results);
  // setTimeout(function() {
  //   if (aggregate.error) return cb(aggregate.error);
  //   cb(null, aggregate.results);
  // }, 0);
};

/**
 * Count
 */
Database.prototype.count = function(collectionName, options, cb) {
  var searchObj = {
    index: this.config.index,
    body: _getQuery(options)
  };
  this.client.count(searchObj, function(error, response) {
    if(error || !response) return cb(error)
    cb(null, response.count);
  });

  // Filter Data based on Options criteria
  // var resultSet = waterlineCriteria(collectionName, this.data, options);
  // // Process Aggregate Options
  // var aggregate = new Aggregate(options, resultSet.results);
  // setTimeout(function() {
  //   if (aggregate.error) return cb(aggregate.error);
  //   cb(null, aggregate.results);
  // }, 0);
};

/**
 * Insert A Record
 */
Database.prototype.insert = function(collectionName, values, cb) {
  // console.log("insert", collectionName, values, this.config);
  var self = this;
  var doc = {
      index: self.config.index,
      type: collectionName,
      id: values.id,
      body: {
        doc: values,
        "doc_as_upsert": true
      }
    }

  self.client.update(doc,
    function(error, response) { //error, response
      if (error) {
        cb(error)
      }
      if (response) {
        cb(null, values);
      }
    }
  );

  // var originalValues = _.clone(values);
  // if (!Array.isArray(values)) values = [values];

  // // To hold any uniqueness constraint violations we encounter:
  // var constraintViolations = [];

  // // Iterate over each record being inserted, deal w/ auto-incrementing
  // // and checking the uniquness constraints.
  // for (var i in values) {
  //   var record = values[i];

  //   // Check Uniqueness Constraints
  //   // (stop at the first failure)
  //   constraintViolations = constraintViolations.concat(self.enforceUniqueness(collectionName, record));
  //   if (constraintViolations.length) break;

  //   // Auto-Increment any values that need it
  //   record = self.autoIncrement(collectionName, record);
  //   record = self.serializeValues(collectionName, record);

  //   if (!self.data[collectionName]) return cb(Errors.CollectionNotRegistered);
  //   self.data[collectionName].push(record);
  // }

  // // If uniqueness constraints were violated, send back a validation error.
  // if (constraintViolations.length) {
  //   return cb(new UniquenessError(constraintViolations));
  // }

  // setTimeout(function() {
  //   cb(null, Array.isArray(originalValues) ? values : values[0]);
  // }, 0);
};

/**
 * Update A Record
 */
Database.prototype.update = function(collectionName, options, values, cb) {
  // console.log("update", collectionName, options, values);
  var self = this;

  self.client.update({
      index: self.config.index,
      type: collectionName,
      id: values.id,
      body: {
        doc: values,
        "doc_as_upsert": true
      }
    },
    function(error, response) { //error, response
      if (error) {
        cb(error)
      }
      if (response) {
        // console.log("update", [values]);
        cb(null, [values]);
      }
    }
  );

  // // Filter Data based on Options criteria
  // var resultSet = waterlineCriteria(collectionName, this.data, options);

  // // Enforce uniquness constraints
  // // If uniqueness constraints were violated, send back a validation error.
  // var violations = self.enforceUniqueness(collectionName, values);
  // if (violations.length) {
  //   return cb(new UniquenessError(violations));
  // }

  // // Otherwise, success!
  // // Build up final set of results.
  // var results = [];
  // for (var i in resultSet.indices) {
  //   var matchIndex = resultSet.indices[i];
  //   var _values = self.data[collectionName][matchIndex];

  //   // Clone the data to avoid providing raw access to the underlying
  //   // in-memory data, lest a user makes inadvertent changes in her app.
  //   self.data[collectionName][matchIndex] = _.extend(_values, values);
  //   results.push(_.cloneDeep(self.data[collectionName][matchIndex]));
  // }

  // setTimeout(function() {
  //   cb(null, results);
  // }, 0);
};

/**
 * Destroy A Record
 */
Database.prototype.destroy = function(collectionName, options, cb) {
  // console.log("destroy", collectionName, options.where.id);
  var self = this;

  self.client.delete({
      index: self.config.index,
      type: collectionName,
      id: options.where.id
    },
    function(error, response) { //error, response
      if (error) {
        cb(error)
      }
      if (response) {
        cb(null, [{
          id: options.where.id
        }]);
      }
    }
  );
  // // Filter Data based on Options criteria
  // var resultSet = waterlineCriteria(collectionName, this.data, options);

  // this.data[collectionName] = _.reject(this.data[collectionName], function(model, i) {
  //   return _.contains(resultSet.indices, i);
  // });

  // setTimeout(function() {
  //   cb(null, resultSet.results);
  // }, 0);
};
