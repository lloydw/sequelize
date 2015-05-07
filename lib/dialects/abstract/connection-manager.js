'use strict';

var Pooling = require('generic-pool')
  , Promise = require('../../promise')
	, ConnectionPool = require('./connection-pool')
  , _ = require('lodash')
  , defaultPoolingConfig = {
    max: 5,
    min: 0,
    idle: 10000,
    handleDisconnects: true
  }
  , ConnectionManager;

ConnectionManager = function(dialect, sequelize) {
  var config = sequelize.config;

  this.sequelize = sequelize;
  this.config = config;
  this.dialect = dialect;

  if (config.pool) {
    config.pool = _.clone(config.pool); // Make sure we don't modify the existing config object (user might re-use it)
    config.pool =_.defaults(config.pool, defaultPoolingConfig, {
      validate: this.$validate.bind(this)
    }) ;
  } else {
    // If the user has turned off pooling we provide a 0/1 pool for backwards compat
    config.pool = _.defaults({
      max: 1,
      min: 0
    }, defaultPoolingConfig, {
      validate: this.$validate.bind(this)
    });
  }

  // Map old names
  if (config.pool.maxIdleTime) config.pool.idle = config.pool.maxIdleTime;
  if (config.pool.maxConnections) config.pool.max = config.pool.maxConnections;
  if (config.pool.minConnections) config.pool.min = config.pool.minConnections;

  this.onProcessExit = this.onProcessExit.bind(this); // Save a reference to the bound version so we can remove it with removeListener
  process.on('exit', this.onProcessExit);
};

ConnectionManager.prototype.onProcessExit = function() {
  var self = this;

  if (this.pool) {
    this.pool.drain(function() {
      self.pool.destroyAllNow();
    });
  }
};

ConnectionManager.prototype.close = function () {
  this.onProcessExit();
  process.removeListener('exit', this.onProcessExit); // Remove the listener, so all references to this instance can be garbage collected.

  this.getConnection = function () {
    return Promise.reject(new Error('ConnectionManager.getConnection was called after the connection manager was closed!'));
  };
};

// This cannot happen in the constructor because the user can specify a min. number of connections to have in the pool
// If he does this, generic-pool will try to call connect before the dialect-specific connection manager has been correctly set up
ConnectionManager.prototype.initPools = function () {
	
  var self = this
    , config = _.cloneDeep(this.config);

  if (!config.shards) {
    this.pool = Pooling.Pool({
      name: 'sequelize-connection',
      create: function(callback) {
        self.$connect(config).nodeify(function (err, connection) {
          callback(err, connection); // For some reason this is needed, else generic-pool things err is a connection or some shit
        });
      },
      destroy: function(connection) {
        self.$disconnect(connection);
      },
      max: config.pool.max,
      min: config.pool.min,
      validate: config.pool.validate,
      idleTimeoutMillis: config.pool.idle
    });
    return;
  }
	
	this.pool = new ConnectionPool(this.config, this.dialect);
};

ConnectionManager.prototype.getConnection = function(options) {
  var self = this;
  options = options || {};

  return new Promise(function (resolve, reject) {
    self.pool.acquire(function(err, connection) {
      if (err) return reject(err);
      resolve(connection);
    }, options.priority, options.type, ('shardName' in options) ? options.shardName : self.config.defaultShardName);
  });
};
ConnectionManager.prototype.releaseConnection = function(connection) {
  var self = this;

  return new Promise(function (resolve, reject) {
    self.pool.release(connection);
    resolve();
  });
};

ConnectionManager.prototype.$connect = function(config) {
  return this.dialect.connectionManager.connect(config);
};
ConnectionManager.prototype.$disconnect = function(connection) {
  return this.dialect.connectionManager.disconnect(connection);
};

ConnectionManager.prototype.$validate = function(connection) {
  if (!this.dialect.connectionManager.validate) return Promise.resolve();
  return this.dialect.connectionManager.validate(connection);
};

module.exports = ConnectionManager;
