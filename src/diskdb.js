var diskDB = require('diskdb');

module.exports = {
  register: function(that) {
    that.db = new DiskDB();
  }
};

function DiskDB(path) {
  this.open = function(name, collections, uniqueKeys) {
    this.db = diskDB.connect('./db', collections.concat('__keys').map(function(s) { return name+'__'+s }));
    this.name = name;
    this.collections = collections;
    this.uniqueKeys = uniqueKeys;
  }
  this.getStore = function(name) {
    return new DbStore(this.db, this.name+'__'+name, this.uniqueKeys[name], this.name+'____keys');
  }
  
  this.clear = function(name) {
    this.db[this.name+'__'+name].remove();
    this.db.loadCollections([ this.name+'__'+name ]);
  }
}

function DbStore(db, name, key, keyName) {
  this.db = db;
  this.name = name;
  this.collection = this.db[this.name];
  this.key = key.name;
  this.keyName = keyName;
  this.autoInc = key.autoIncrement;
  
  this.insert = function(object) {
    this.applyKey(object);
    var pattern = {}; pattern[this.key] = object[this.key];
    if(!this.collection.findOne(pattern)) {
      this.collection.save(object);
      return { result: object[this.key] };
    }
    else return { error: 'object is not unique, ' + this.key + '=' + object[this.key] };
  }
  
  this.put = function(object) {
    var pattern = {}; pattern[this.key] = object[this.key];
    this.collection.update(pattern, object, { upsert: true });
  }
  
  this.getAll = function() {
    return this.collection.find();
  }
  
  this.getOne = function(property, value) {
    var pattern = {}; pattern[property] = value;
    return this.collection.findOne(pattern);
  }
  
  this.delete = function(property, value) {
    var pattern = {}; pattern[property] = value;
    return this.collection.remove(pattern, false);
  }
  
  this.applyKey = function(object) {
    if(object[this.key] || !this.autoInc) return;
    var last = this.db[this.keyName].findOne({ name: 'keys' });
    last = last ? last.key : 0;
    this.db[this.keyName].update({ name: 'keys' }, { name: 'keys', key: last+1 }, { upsert: true });
    object[this.key] = last+1;
  }
}
