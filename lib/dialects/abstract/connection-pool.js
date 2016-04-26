'use strict';

var Pooling = require('generic-pool')
  , _ = require('lodash');

var ConnectionPool = function(config, dialect) {
  this.config = config;
  this.dialect = dialect;
  this.pools = {};

  // Duplicate the base config into each database
  var defaultConfig = _.extend({}, config);
  delete defaultConfig.databases;

  for (var key in config.databases) {
    config.databases[key] = _.extend({}, defaultConfig, config.databases[key]);
  }
};

ConnectionPool.prototype.acquire = function(callback, priority, queryType, poolName) {
  this.getPool(poolName).acquire(callback, priority);
};

ConnectionPool.prototype.release = function(connection) {
  return this.pools[connection.poolName].release(connection);
};

ConnectionPool.prototype.destroy = function(connection) {
  return this.pools[connection.poolName].destroy(connection);
};

ConnectionPool.prototype.destroyAllNow = function() {
  for (var key in this.pools) {
    this.pools[key].destroyAllNow();
  }
};

ConnectionPool.prototype.drain = function(callback) {
  var pending = 0;
  for (var key in this.pools) {
    pending++;
    this.pools[key].drain(function() {
      pending--;
      if (pending === 0) {
        callback();
      }
    });
  }
};

ConnectionPool.prototype.getPool = function(name) {
  var self = this;
  if (name in self.pools) {
    return self.pools[name];
  }

  if (!(name in self.config.databases)) {
    throw new Error('Unknown database ' + name);
  }

  var pool = Pooling.Pool({
    name: 'sequelize-connection-' + name,
    create: function(callback) {
      var config = self.config.databases[name];
      self.dialect.connectionManager.connect(config).tap(function (connection) {
        connection.poolName = name;
      }).nodeify(function (err, connection) {
        callback(err, connection); // For some reason this is needed, else generic-pool things err is a connection or some shit
      });
    },
    destroy: function(connection) {
      self.dialect.connectionManager.disconnect(connection);
    },
    validate: self.config.pool.validate,
    max: self.config.pool.max,
    min: self.config.pool.min,
    idleTimeoutMillis: self.config.pool.idle
  });

  this.pools[name] = pool;
  return pool;
};

module.exports = ConnectionPool;