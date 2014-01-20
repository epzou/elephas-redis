'use strict';

var redis = require('redis');
var crc = require('crc');
var randy = require('randy');
var HashRing = require('hashring');
var _ = require('lodash');

/**
 * initialize Redis.
 */
var Redis = function() {

  var self = this;

  self.mapping = {};
  self.clusters =  {};
  self.nodes =  {};
  self.clients = {};
  self.hashring = {};
};

Redis.prototype.configure = function(opts, logger) {

  console.log(opts);

  var self = this;

  self.parseOpts(opts);

  self.mapping = opts.mapping || {};
  self.clusters = opts.clusters || {};
  self.nodes = opts.nodes || {};

  // Nodes毎にコネクションプーリング
  var clients = {};
  _.each(self.nodes, function(node, key) {
    var client = redis.createClient(
      node.port,
      node.host,
      {
        retry_max_delay: 60 * 1000
      }
    );
    // Error
    client.on('error', function(err) {
      if (logger) {
        logger.get('redis').error('Connection failed.');
        logger.get('redis').error(err);
      }
    });
    clients[key] = client;
  });
  self.clients = clients;

  var hashring = {};
  _.each(self.clusters, function(cluster, key) {
    hashring[key] = new HashRing(
      cluster,
      'md5',
      { 'max cache size': 10000 }
    );
  });
  this.hashring = hashring;
};

Redis.prototype.clean = function() {

  var self = this;

  _.each(self.clients, function(client) {
    if (client) {
      client.quit();
    }
  });

};

Redis.prototype.parseOpts = function(opts) {

  if (opts && opts.mapping) {
    _.each(opts.mapping, function(map) {
      if (opts.clusters && opts.clusters[map.clusterName]) {
        map.cluster = opts.clusters[map.clusterName];
      }
    });
  }
};

Redis.prototype.getNode = function(schemaName, key, callback) {

  var self = this;

  var schema = self.mapping[schemaName];

  var cluster = schema.cluster;

  if(_.size(cluster) === 1) {
    if (!callback) {
      callback = key;
    }
    return callback(null, self.clients[cluster[0]]);
  }
  if (!schema.algorithm) {
    return callback('algorithm is not defined.');
  }

  var capitalize = schema.algorithm.replace(/^\w/, function($0) { return $0.toUpperCase();});
  var chooseFunc = self['choose' + capitalize];

  if (typeof chooseFunc !== 'function') {
    return callback('algorithm contains only "crc", "hashring", "random", "manual"');
  }

  chooseFunc.bind(self)(schemaName, key, callback);
};

Redis.prototype.chooseCrc = function(schemaName, key, callback) {

  var self = this;

  var cluster = self.mapping[schemaName].cluster;

  var num = crc.crc16(key) % _.size(cluster);

  var node = cluster[num];

  callback(null, self.clients[node]);

};

Redis.prototype.chooseHashring = function(schemaName, key, callback) {

  var self = this;

  var clusterName = self.mapping[schemaName].clusterName;

  var node = self.hashring[clusterName].get(key);

  callback(null, self.clients[node]);
};

Redis.prototype.chooseRandom = function(schemaName, key, callback) {

  var self = this;

  var cluster = self.mapping[schemaName].cluster;

  var num = Math.round(randy.random() * (_.size(cluster) - 1));

  var node = cluster[num];

  callback(null, self.clients[node]);
};

Redis.prototype.chooseManual = function(schemaName, key, callback) {

  var self = this;

  var cluster = self.mapping[schemaName].cluster;

  if (typeof key === 'function') {

    if (!callback) {
      return callback(null, self.clients[cluster[0]]);
    }

    var func = key;
    func.bind(self)(function(err, key) {
      if (err) {
        return callback(err);
      }
      callback(null, self.clients[cluster[key]]);
    });
  } else if (typeof key === 'number') {
    callback(null, self.clients[cluster[key]]);
  } else {
    // Error
    console.log('Error !!!');
  }

};

module.exports = new Redis();