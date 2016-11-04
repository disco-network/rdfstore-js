var Tree = require('./btree').Tree;
var utils = require('./utils');
var async = utils;
var InMemoryLexicon = require('./lexicon').Lexicon;
var diskDB = require('./diskdb');

/**
 * Temporal implementation of the lexicon
 */

//TODO: unique indices
Lexicon = function(callback, dbName){
    var that = this;

    diskDB.register(that);

    this.defaultGraphOid = 0;
    this.defaultGraphUri = "https://github.com/antoniogarrote/rdfstore-js#default_graph";
    this.defaultGraphUriTerm = {"token":"uri","prefix":null,"suffix":null,"value":this.defaultGraphUri};
    this.oidCounter = 1;

    that.dbName = dbName || "rdfstorejs";
    that.db.open(this.dbName+"_lexicon", [ 'knownGraphs', 'uris', 'blanks', 'literals' ], { 
      'knownGraphs': { name: 'oid' }, 
      'uris': { name: 'id', autoIncrement: true },
      'blanks': { name: 'id', autoIncrement: true },
      'literals': { name: 'id', autoIncrement: true },
    });
    callback(that);
};

/**
 * Registers a new graph in the lexicon list of known graphs.
 * @param oid
 * @param uriToken
 * @param callback
 */
Lexicon.prototype.registerGraph = function(oid, uriToken, callback){
    if(oid != this.defaultGraphOid) {
        var objectStore = this.db.getStore('knownGraphs');
        var result = objectStore.insert({oid: oid, uriToken: uriToken});
        if(result.error) callback(null, result.error);
        else callback(true);
    } else {
        callback();
    }
};

/**
 * Returns the list of known graphs OIDs or URIs.
 * @param returnUris
 * @param callback
 */
Lexicon.prototype.registeredGraphs = function(returnUris, callback) {
    var graphs = [];
    var objectStore = this.db.getStore("knownGraphs");

    graphs = objectStore.getAll().map(function(object) {
        if(returnUris === true) return object.uriToken;
        else return object.oid;
    });
    callback(graphs);
};

/**
 * Registers a URI in the lexicon. It returns the allocated OID for the URI.
 * As a side effect it increases the cost counter for that URI if it is already registered.
 * @param uri
 * @param callback
 * @returns URI's OID.
 */
Lexicon.prototype.registerUri = function(uri, callback) {
    var that = this;
    if(uri === this.defaultGraphUri) {
        callback(this.defaultGraphOid);
    } else{
        var objectStore = that.db.getStore("uris");
        var result = objectStore.getOne("uri",uri);
        
        var uriData = result;
        if(uriData) {
            uriData.counter++;
            var oid = uriData.id;
            objectStore.put(uriData);
            callback(oid);
        }
        else {
          var result = objectStore.insert({ uri: uri, counter: 0 });
          if(result.error) callback(null, new Error("Error inserting the URI data " + result.error));
          else callback(result.result);
        }
    }
};

/**
 * Returns the OID associated to the URI.
 * If the URI hasn't been  associated in the lexicon, -1 is returned.
 * @param uri
 * @param callback
 */
Lexicon.prototype.resolveUri = function(uri,callback) {
    if(uri === this.defaultGraphUri) {
        callback(this.defaultGraphOid);
    } else {
        var objectStore = this.db.getStore("uris");
        var result = objectStore.getOne("uri",uri);
        if(result != null) callback(result.id);
        else callback(-1);
    }
};

/**
 * Returns the cost associated to the URI.
 * If the URI hasn't been associated in the lexicon, -1 is returned.
 * @param uri
 * @returns {*}
 */
Lexicon.prototype.resolveUriCost = function(uri, callback) {
    if(uri === this.defaultGraphUri) {
        callback(0);
    } else {
        var objectStore = that.db.getStore("uris");
        var result = objectStore.getOne("uri",uri);
        if(result != null) callback(result.cost);
        else callback(-1);
    }
};

/**
 * Register a new blank node in the lexicon.
 * @param label
 * @returns {string}
 */
Lexicon.prototype.registerBlank = function(callback) {
    var oidStr = guid();
    var that = this;

    var objectStore = that.db.getStore("blanks");
    var requestAdd = objectStore.insert({label: oidStr, counter:0});
    if(requestAdd.error)
        callback(null, new Error("Error inserting the URI data"+requestAdd.error));
    else
        callback(requestAdd.result);
};

/**
 * Resolves a blank node OID
 * @param oid
 * @param callback
 */
//Lexicon.prototype.resolveBlank = function(oid,callback) {
//    var that = this;
//    var objectStore = that.db.transaction(["blanks"]).objectStore("blanks");
//    var request = objectStore.get(oid);
//    request.onsuccess = function(event) {
//        if(event.target.result != null)
//            callback(event.target.result.id);
//        else {
//            // we register it if it doesn't exist
//        }
//    };
//    request.onerror = function(event) {
//        callback(null, new Error("Error retrieving blank data "+event.target.errorCode));
//    }
//
//    this.oidBlanks.search(label, function(oidData){
//        if(oidData != null) {
//            callback(oidData);
//        } else {
//            // ??
//            var oid = that.oidCounter;
//            this.oidCounter++;
//            callback(""+oid);
//            //
//        }
//    });
//};

/**
 * Blank nodes don't have an associated cost.
 * @param label
 * @param callback
 * @returns {number}
 */
Lexicon.prototype.resolveBlankCost = function(label, callback) {
    callback(0);
};

/**
 * Registers a new literal in the index.
 * @param literal
 * @param callback
 * @returns the OID of the newly registered literal
 */
Lexicon.prototype.registerLiteral = function(literal, callback) {
    var that = this;

    var objectStore = that.db.getStore("literals");
    var request = objectStore.getOne("literal",literal);
    var literalData = request;
    if(literalData) {
        // found in index -> update
        literalData.counter++;
        var oid = literalData.id;
        var requestUpdate = objectStore.put(literalData);
        callback(oid);
    } else {
        // not found -> create
        var requestAdd = objectStore.insert({literal: literal, counter:0});
        if(requestAdd.error)
            callback(null, new Error('Error inserting the literal data'+requestAdd.error));
        else 
            callback(requestAdd.result);
    }
};

/**
 * Returns the OID of the resolved literal or -1 if no literal is found.
 * @param literal
 * @param callback
 */
Lexicon.prototype.resolveLiteral = function (literal,callback) {
    var objectStore = that.db.getStore("literals");
    var result = objectStore.getOne("literal",literal);
    
    if(result != null) callback(result.id);
    else callback(-1);
};

/**
 * Returns the cost associated to the literal or -1 if no literal is found.
 * @param literal
 * @param callback
 */
Lexicon.prototype.resolveLiteralCost = function (literal,callback) {
    var objectStore = that.db.getStore("literals");
    var request = objectStore.getOne("literal",literal);
    
    if(result != null) callback(result.cost);
    else callback(-1);
};


/**
 * Transforms a literal string into a token object.
 * @param literalString
 * @returns A token object with the parsed literal.
 */
Lexicon.prototype.parseLiteral = function(literalString) {
    return InMemoryLexicon.prototype.parseLiteral(literalString);
};

/**
 * Parses a literal URI string into a token object
 * @param uriString
 * @returns A token object with the parsed URI.
 */
Lexicon.prototype.parseUri = function(uriString) {
    return InMemoryLexicon.prototype.parseUri(uriString);
};

/**
 * Retrieves a token containing the URI, literal or blank node associated
 * to the provided OID.
 * If no value is found, null is returned.
 * @param oid
 * @param callback
 * @returns parsed token or null if not found.
 */
Lexicon.prototype.retrieve = function(oid, callback) {
    var that = this;

    if(oid === this.defaultGraphOid) {
        callback({
            token: "uri",
            value:this.defaultGraphUri,
            prefix: null,
            suffix: null,
            defaultGraph: true
        });
    } else {
        async.seq(function(found,k){
            var result = that.db.getStore("uris").getOne('id', oid);
            if(result != null) k(null, that.parseUri(result.uri));
            else k(null, null);
        }, function(found,k){
            if(found == null) {
                var result = that.db.getStore("literals").getOne('id', oid);
                if(result != null) k(null, that.parseLiteral(result.literal));
                else k(null, null);
            } else {
                k(null,found);
            }
        }, function(found,k){
            if(found == null) {
                    var result = that.db.getStore("blanks").getOne('id', oid);
                    if(result != null) {
                        var label = '_:' + result.id;
                        k(null, that.parseLiteral({token:'blank', value: label}));
                    }
                    else k(null, null);
            } else {
                k(null,found);
            }
        })(null,function(err,found){
            if(err)
                callback(null,err);
            else
                callback(found);
        });
    }
};

/**
 * Empties the lexicon and restarts the counters.
 * @param callback
 */
Lexicon.prototype.clear = function(callback) {
    var that = this;
    this.defaultGraphOid = 0;
    this.defaultGraphUri = "https://github.com/antoniogarrote/rdfstore-js#default_graph";
    this.defaultGraphUriTerm = {"token":"uri","prefix":null,"suffix":null,"value":this.defaultGraphUri};
    
    that.db.clear('uris');
    that.db.clear('literals');
    that.db.clear('blanks');
    
    if(callback != null) callback();
};

/**
 * Removes the values associated to the subject, predicate, object and graph
 * values of the provided quad.
 * @param quad
 * @param key
 * @param callback
 */
Lexicon.prototype.unregister = function (quad, key, callback) {
    var that = this;
    async.seq(function(k){
        that._unregisterTerm(quad.subject.token, key.subject,k);
    }, function(k){
        that._unregisterTerm(quad.predicate.token, key.predicate,k);
    }, function(k){
        that._unregisterTerm(quad.object.token, key.object, k);
    }, function(k){
        if (quad.graph != null) {
            that._unregisterTerm(quad.graph.token, key.graph, k);
        } else {
            k();
        }
    })(function(){
        callback(true);
    });
};

/**
 * Unregisters a value, either URI, literal or blank.
 * @param kind
 * @param oid
 * @param callback
 * @private
 */
Lexicon.prototype._unregisterTerm = function (kind, oid, callback) {
    var that = this;
    //var transaction = that.db.transaction(["uris","literals","blanks", "knownGraphs"],"readwrite"), request;
    if (kind === 'uri') {
        if (oid != this.defaultGraphOid) {
            var removeKnownGraphs = function() {
                that.db.getStore("knownGraphs").delete('oid', oid);
                callback();
            };
            that.db.getStore("uris").delete('id', oid);
            removeKnownGraphs();
        } else {
            callback();
        }
    } else if (kind === 'literal') {
        that.db.getStore("literals").delete('id', oid);
        callback();
    } else if (kind === 'blank') {
        that.db.getStore("blanks").delete('id', oid);
        callback();
    } else {
        callback();
    }
};

module.exports = {
    PersistentLexicon: Lexicon
};
