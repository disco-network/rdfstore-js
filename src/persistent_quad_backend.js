
// imports
var utils = require('./utils');
var diskDB = require('./diskdb');
var _ = utils;
var async = utils;

/*
 * "perfect" indices for RDF indexing
 *
 * SPOG (?, ?, ?, ?), (s, ?, ?, ?), (s, p, ?, ?), (s, p, o, ?), (s, p, o, g)
 * GP   (?, ?, ?, g), (?, p, ?, g)
 * OGS  (?, ?, o, ?), (?, ?, o, g), (s, ?, o, g)
 * POG  (?, p, ?, ?), (?, p, o, ?), (?, p, o, g)
 * GSP  (s, ?, ?, g), (s, p, ?, g)
 * OS   (s, ?, o, ?)
 *
 * @param configuration['dbName'] Name for the db
 * @return The newly created backend.
 */
QuadBackend = function (configuration, callback) {
    var that = this;

    if (arguments !== 0) {

        diskDB.register(that);

        this.indexMap = {};
        this.indices = ['SPOG', 'GP', 'OGS', 'POG', 'GSP', 'OS'];
        this.componentOrders = {
            SPOG:['subject', 'predicate', 'object', 'graph'],
            GP:['graph', 'predicate', 'subject', 'object'],
            OGS:['object', 'graph', 'subject', 'predicate'],
            POG:['predicate', 'object', 'graph', 'subject'],
            GSP:['graph', 'subject', 'predicate', 'object'],
            OS:['object', 'subject', 'predicate', 'graph']
        };

        that.dbName = configuration['name'] || "rdfstorejs";
        var keys = {}; keys[that.dbName] = { name: 'SPOG' }
        var request = that.db.open(this.dbName+"_db", [ that.dbName ], keys);
        
        callback(that);
    }
};


QuadBackend.prototype.index = function (quad, callback) {
    var that = this;
    _.each(this.indices, function(index){
        quad[index] = that._genMinIndexKey(quad, index);
    });
    
    var objectStore = that.db.getStore(that.dbName);
    var request = objectStore.insert(quad);
    if(!request.error) {
        callback(true);
    }
    else callback(request.error);
};

QuadBackend.prototype.range = function (pattern, callback) {
    var that = this;
    var objectStore = that.db.getStore(that.dbName);
    var indexKey = this._indexForPattern(pattern);
    var minIndexKeyValue = this._genMinIndexKey(pattern,indexKey);
    var maxIndexKeyValue = this._genMaxIndexKey(pattern,indexKey);
    var quads = [];

    var tmp = objectStore.getAll();
    _.each(tmp, function(el) {
      if(that.compareIndices(el[indexKey], minIndexKeyValue) !== -1
        && that.compareIndices(el[indexKey], maxIndexKeyValue) !== 1)
        quads.push(el);
    })
    callback(quads);
};

QuadBackend.prototype.compareIndices = function (left, right) {
    var leftIndexParts = left.split(".");
    var rightIndexParts = right.split(".");

    for (var i = 0; i < leftIndexParts.length; ++i) {
      if (leftIndexParts[i] < rightIndexParts[i]) {
        return -1;
      }
      else if (rightIndexParts[i] > rightIndexParts[i]) {
        return 1;
      }
      else continue;
    }

    return 0;
};

QuadBackend.prototype.search = function (quad, callback) {
    var that = this;
    var objectStore = that.db.getStore(that.dbName);
    var indexKey = this._genMinIndexKey(quad, 'SPOG');
    var result = objectStore.getOne('SPOG', indexKey);
    callback(result != null);
};


QuadBackend.prototype.delete = function (quad, callback) {
    var that = this;
    var indexKey = that._genMinIndexKey(quad, 'SPOG');
    var request = that.db
        .getStore(that.dbName)
        .delete('SPOG', indexKey);
    callback(true);
};

QuadBackend.prototype._genMinIndexKey = function(quad,index) {
    var indexComponents = this.componentOrders[index];
    return _.map(indexComponents, function(component){
        if(typeof(quad[component]) === 'string' || quad[component] == null) {
            return "-1";
        } else {
            return ""+quad[component];
        }
    }).join('.');
};

QuadBackend.prototype._genMaxIndexKey = function(quad,index) {
    var indexComponents = this.componentOrders[index];
    var acum = [];
    var foundFirstMissing = false;
    for(var i=0; i<indexComponents.length; i++){
        var component = indexComponents[i];
        var componentValue= quad[component];
        if(typeof(componentValue) === 'string') {
            if (foundFirstMissing === false) {
                    foundFirstMissing = true;
                if (i - 1 >= 0) {
                    acum[i - 1] = acum[i - 1] + 1
                }
            }
            acum[i] = -1;
        } else {
            acum[i] = componentValue;
        }
    }
    return _.map(acum, function(componentValue){
        return ""+componentValue
    }).join('.');
};


QuadBackend.prototype._indexForPattern = function (pattern) {
    var indexKey = pattern.indexKey;

    for (var i = 0; i < this.indices.length; i++) {
        var index = this.indices[i];
        var indexComponents = this.componentOrders[index];
        for (var j = 0; j < indexComponents.length; j++) {
            if (_.include(indexKey, indexComponents[j]) === false) {
                break;
            }
            if (j == indexKey.length - 1) {
                return index;
            }
        }
    }

    return 'SPOG'; // If no other match, we return the more generic index
};


QuadBackend.prototype.clear = function(callback) {
    var that = this;
    request = that.db.clear(that.dbName);
};

module.exports.PersistentQuadBackend = QuadBackend;
