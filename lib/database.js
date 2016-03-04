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
    if (!exists) {
      console.log('creating index')
      return self.client.indices.create({
        index: self.config.index,
        body: {
          "settings": {
            "number_of_shards": 2,
            "number_of_replicas": 1
          }
        }
      });
    } 
   }).catch(function(err) {
    throw new Error(err)
  });
  return this;
};

/**
 * Select
 */
Database.prototype.select = function(collectionName, options, cb) {
  // console.log("select", collectionName, options);
  var searchObj = {
    index: this.config.index
  };
  if (options.where && options.where.id) {
    // Get ONE
    searchObj.id = options.where.id.toString();
    this.client.get(searchObj, function(error, response) {
      cb(null, response._source);
    });

  } else {
    // Get Query
    var query = options
    var _q = {
          query: {
              filtered: {
                  query: {
                      bool: {
                          should: []
                      }
                  },
                  filter: {
                    term: {
                      model: query.where.model
                    }
                  }
              }
          }
      };
    if(query.where.model) delete query.where.model
      var util = require('util')
    if (query.where && query.where != null && query.where.hasOwnProperty('or')) {
        query.where.or.forEach(function(o) {
          var t = { match: {} }
          t.match[_.keys(o)[0]] = _.values(o)[0]
          var m = {bool: {must: [t]}}
          console.log('m', m)
          _q.query.filtered.query.bool.should.push(m)
        })
    } else {
        delete _q.query.filtered.query.bool
        _q.query.filtered.query.match = query.where 
    }

    if (query.hasOwnProperty('limit')) {
        _q.size = query.limit;
    }

    if (query.hasOwnProperty('skip')) {
        _q.from = query.skip;
    }

    if (query.hasOwnProperty('sort')) {
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

    searchObj.body = _q;
    this.client.search(searchObj, function(error, response) {
      if(!response || !response.hits || !response.hits.hits) return cb(error)
      var results = _.map(response.hits.hits, function(hit) {
        return hit._source;
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
 * Insert A Record
 */
Database.prototype.insert = function(collectionName, values, cb) {
  console.log("insert", collectionName, values, this.config);
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
  console.log(doc)
  self.client.update(doc,
    function(error, response) { //error, response
      if (error) {
        console.log("inserterror", error);
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
        console.log("updateerror", error);
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
        console.log("destroyerror", error);
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