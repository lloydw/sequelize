'use strict';

var Pooling = require('generic-pool')
  , Promise = require('../../promise')
  , ConnectionPool = require('./connection-pool')
  , _ = require('lodash')
  , semver = require('semver')
  , defaultPoolingConfig = {
    max: 5,
    min: 0,
    idle: 10000,
    handleDisconnects: true
  }
  , ConnectionManager;

ConnectionManager = function(dialect, sequelize) {
  var config = _.cloneDeep(sequelize.config);

  this.sequelize = sequelize;
  this.config = config;
  this.dialect = dialect;
  this.versionPromise = null;
  this.dialectName = this.sequelize.options.dialect;

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

ConnectionManager.prototype.refreshTypeParser = function(dataTypes) {
  _.each(dataTypes, function (dataType, key) {
    if (dataType.hasOwnProperty('parse')) {
      var dialectName = this.dialectName;
      if (dialectName === 'mariadb') {
        dialectName = 'mysql';
      }

      if (dataType.types[dialectName]) {
        this.$refreshTypeParser(dataType);
      } else {
        throw new Error('Parse function not supported for type ' + dataType.key + ' in dialect ' + this.dialectName);
      }
    }
  }.bind(this));
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
    , config = this.config;

  if (!config.databases) {
    this.pool = Pooling.Pool({
      name: 'sequelize-connection',
      create: function(callback) {
        self.$connect(config).nodeify(function (err, connection) {
          callback(err, connection); // For some reason this is needed, else generic-pool things err is a connection or some shit
        });
      },
      destroy: function(connection) {
        self.$disconnect(connection);
        return null;
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

  var promise;
  if (this.sequelize.options.databaseVersion === 0) {
    if (this.versionPromise) {
      promise = this.versionPromise;
    } else {
      promise = this.versionPromise = self.$connect(self.config.databases ? self.config.databases[options.dbName] : self.config).then(function (connection) {
        var _options = {};
        _options.transaction = { connection: connection }; // Cheat .query to use our private connection
        _options.logging = function () {};
        _options.logging.__testLoggingFn = true;

        return self.sequelize.databaseVersion(_options).then(function (version) {
          self.sequelize.options.databaseVersion = semver.valid(version) ? version : self.defaultVersion;

          self.versionPromise = null;

          self.$disconnect(connection);
          return null;
        });
      }).catch(function (err) {
        self.versionPromise = null;
        throw err;
      });
    }
  } else {
    promise = Promise.resolve();
  }

  return promise.then(function () {
    return new Promise(function (resolve, reject) {
      self.pool.acquire(function(err, connection) {
        if (err) return reject(err);
        resolve(connection);
      }, options.priority, options.type, options.dbName);
    });
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
  return this.sequelize.runHooks('beforeConnect', config).bind(this).then(function () {
    return this.dialect.connectionManager.connect(config).then(function (connection) {
      return connection;
    });
  });
};
ConnectionManager.prototype.$disconnect = function(connection) {
  return this.dialect.connectionManager.disconnect(connection);
};

ConnectionManager.prototype.$validate = function(connection) {
  if (!this.dialect.connectionManager.validate) return Promise.resolve();
  return this.dialect.connectionManager.validate(connection);
};

module.exports = ConnectionManager;
