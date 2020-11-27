'use strict';

var Y = require('yjs');
var binary = require('lib0/dist/binary.cjs');
var encoding = require('lib0/dist/encoding.cjs');
var buffer = require('buffer');
var promise = require('lib0/dist/promise.cjs');
var t = require('lib0/dist/testing.cjs');
var environment_js = require('lib0/dist/environment.cjs');

const PREFERRED_TRIM_SIZE = 400;

/**
 * @param {any} db
 * @param {string} docName
 * @param {number} from Greater than or equal
 * @param {number} to lower than (not equal)
 * @return {Promise<void>}
 */
const clearUpdatesRange = async (db, docName, from, to) => db.del({
  docName,
  clock: {
    $gte: from,
    $lt: to
  }
});

/**
 * @param {any} db
 * @param {string} docName
 * @param {Uint8Array} stateAsUpdate
 * @param {Uint8Array} stateVector
 * @return {Promise<number>} returns the clock of the flushed doc
 */
const flushDocument = async (db, docName, stateAsUpdate, stateVector) => {
  const clock = await storeUpdate(db, docName, stateAsUpdate);
  await writeStateVector(db, docName, stateVector, clock);
  await clearUpdatesRange(db, docName, 0, clock);
  return clock
};

/**
 * Create a unique key for a update message.
 * @param {string} docName
 * @param {number} clock must be unique
 * @return {Object} [opts.version, opts.docName, opts.action, opts.clock]
 */
const createDocumentUpdateKey = (docName, clock) => ({
  version: 'v1',
  action: 'update',
  docName,
  clock
});

/**
 * @param {string} docName
 * @return {Object} [opts.docName, opts.version]
 */
const createDocumentStateVectorKey = docName => {
  return {
    docName: docName,
    version: 'v1_sv'
  }
};

/**
 * Level expects a Buffer, but in Yjs we typically work with Uint8Arrays.
 *
 * Since Level thinks that these are two entirely different things,
 * we transform the Uint8array to a Buffer before storing it.
 *
 * @param {any} db
 * @param {Object} values
 */
const mongoPut = async (db, values) => await db.put(values);

/**
 * @param {any} db
 * @param {object} query
 * @param {object} opts
 * @return {Promise<Array<any>>}
 */
const getMongoBulkData = async (db, query, opts) => await db.readAsCursor(query, opts);

/**
 * @param {any} db
 * @return {Promise<any>}
 */
const flushDB = db => db.flush();

/**
 * Get all document updates for a specific document.
 *
 * @param {any} db
 * @param {string} docName
 * @param {any} [opts]
 * @return {Promise<Array<Object>>}
 */
const getMongoUpdates = async (db, docName, opts = {}) => await getMongoBulkData(db, {
  ...createDocumentUpdateKey(docName, 0),
  clock: {
    $gte: 0,
    $lt: binary.BITS32
  }
},
opts
);

/**
 * @param {any} db
 * @param {string} docName
 * @return {Promise<number>} Returns -1 if this document doesn't exist yet
 */
const getCurrentUpdateClock = async (db, docName) => await getMongoUpdates(db, docName, {
  reverse: true,
  limit: 1
}).then(updates => {
  if (updates.length === 0) {
    return -1
  } else {
    return updates[0].clock
  }
});

/**
 * @param {any} db
 * @param {string} docName
 * @param {Uint8Array} sv state vector
 * @param {number} clock current clock of the document so we can determine when this statevector was created
 */
const writeStateVector = async (db, docName, sv, clock) => {
  const encoder = encoding.createEncoder();
  encoding.writeVarUint8Array(encoder, sv);
  await mongoPut(db, {
    ...createDocumentStateVectorKey(docName),
    value: buffer.Buffer.from(encoding.toUint8Array(encoder)),
    clock
  });
};

/**
 * @param {any} db
 * @param {string} docName
 * @param {Uint8Array} update
 * @return {Promise<number>} Returns the clock of the stored update
 */
const storeUpdate = async (db, docName, update) => {
  const clock = await getCurrentUpdateClock(db, docName);
  if (clock === -1) {
    const ydoc = new Y.Doc();
    Y.applyUpdate(ydoc, update);
    const sv = Y.encodeStateVector(ydoc);
    await writeStateVector(db, docName, sv, 0);
  }

  await mongoPut(db, {
    ...createDocumentUpdateKey(docName, clock + 1),
    value: buffer.Buffer.from(update)
  });

  return clock + 1
};

/**
 * @param {Array<Uint8Array>} updates
 * @return {{update:Uint8Array, sv: Uint8Array}}
 */
const mergeUpdates = (updates) => {
  const ydoc = new Y.Doc();
  ydoc.transact(() => {
    for (let i = 0; i < updates.length; i++) {
      Y.applyUpdate(ydoc, updates[i]);
    }
  });
  return { update: Y.encodeStateAsUpdate(ydoc), sv: Y.encodeStateVector(ydoc) }
};

const { MongoClient } = require('mongodb');

class MongoAdapter {
  constructor (location, dbName, collection) {
    this.location = location;
    this.dbName = dbName;
    this.collection = collection || 'yjs-writings';
    this.db = null;
    this.open();
  }

  open () {
    const mongojsDb = new MongoClient(this.location, {useUnifiedTopology: true});

    async function connect(){
      try{
        await mongojsDb.connect();

        const db = mongojsDb.db(this.dbName);
        this.db = db;
        console.log("=> Connected to Y-MongoDB");
      }finally{
        await mongojsDb.close();
      }
    }

    connect();
  }

  async get (query) {
    return await this.db.collection(this.collection).findOne(query)
  }

  async put (values) {
    if (!values.docName && !values.version && !values.value) { throw new Error('Document and version must be provided') }

    return await this.db.collection(this.collection).save(values)
  }

  async del (query) {
    const bulk = await this.db.collection(this.collection).initializeOrderedBulkOp();
    await bulk.find(query).remove();
    return await bulk.execute()
  }

  async readAsCursor (query, opts = {}) {
    return await this.db.collection(this.collection).find(query).limit(opts.limit).sort({clock: -1}).toArray()
  }

  async close () {
    return await this.db.close()
  }

  async flush () {
    await this.db.dropDatabase();
    await this.db.close();
  }
}

const getUpdates = docs => {
  if (!Array.isArray(docs) || !docs.length) return []

  return docs.map(update => update.value.buffer)
};

class MongodbPersistence {
  /**
   * @param {string} location
   * @param {string} [collection]
   */
  constructor (location, _db, collection) {
    const db = new MongoAdapter(location, _db, collection);
    this.tr = promise.resolve();

    this._transact = f => {
      const currTr = this.tr;
      this.tr = (async () => {
        await currTr;
        let res = /** @type {any} */ (null);
        try {
          res = await f(db);
        } catch (err) {
          console.warn('Error during saving transaction', err);
        }
        return res
      })();
      return this.tr
    };
  }

  /**
   * @param {string} docName
   * @return {Promise<Y.Doc>}
   */
  getYDoc (docName) {
    return this._transact(async db => {
      const docs = await getMongoUpdates(db, docName);
      const updates = getUpdates(docs);
      const ydoc = new Y.Doc();
      ydoc.transact(() => {
        for (let i = 0; i < updates.length; i++) {
          Y.applyUpdate(ydoc, updates[i]);
        }
      });
      if (updates.length > PREFERRED_TRIM_SIZE) {
        await flushDocument(db, docName, Y.encodeStateAsUpdate(ydoc), Y.encodeStateVector(ydoc));
      }
      return ydoc
    })
  }

  /**
   * @param {string} docName
   * @param {Uint8Array} update
   * @return {Promise<number>} Returns the clock of the stored update
   */
  storeUpdate (docName, update) {
    return this._transact(db => storeUpdate(db, docName, update))
  }

  /**
   * @param {string} docName
   * @return {Promise<void>}
   */
  clearDocument (docName) {
    return this._transact(async db => {
      await db.del(createDocumentStateVectorKey(docName));
      await clearUpdatesRange(db, docName, 0, binary.BITS32);
    })
  }

  /**
   * @param {string} docName
   * @return {Promise<void>}
   */
  flushDocument (docName) {
    return this._transact(async db => {
      const docs = await getMongoUpdates(db, docName);
      const updates = getUpdates(docs);
      const { update, sv } = mergeUpdates(updates);
      await flushDocument(db, docName, update, sv);
    })
  }

  flushDB () {
    return this._transact(async db => {
      await flushDB(db);
    })
  }
}

// import * as decoding from 'lib0/decoding.js'

const location = 'mongodb://localhost:27017/y-mongodb-test';
const collection = 'mongodb://localhost:27017/y-transactions';

const flushUpdatesHelper = (ldb, docName, actions) => {
  return Promise.all(actions.map(action => ldb.storeUpdate(action.docName, action.update)))
};


const testMongodbUpdateStorage = async tc => {
  const N = PREFERRED_TRIM_SIZE * 2;

  const docName = tc.testName;
  const ydoc1 = new Y.Doc();
  ydoc1.clientID = 0;
  const dbPersistence = new MongodbPersistence(location, collection);
  await dbPersistence.clearDocument(docName);

  const updates = [];

  ydoc1.on('update', async update => {
    updates.push({
      docName,
      update
    });
  });

  await flushUpdatesHelper(dbPersistence, docName, updates);

  const values = await dbPersistence._transact(db => getMongoUpdates(db, docName));
  for (let i = 0; i < values.length; i++) {
    t.assert(values[i].clock === i);
  }

  const yarray = ydoc1.getArray('arr');
  for (let i = 0; i < N; i++) {
    yarray.insert(0, [i]);
  }

  await dbPersistence.flushDocument(docName);

  const mergedUpdates = await dbPersistence._transact(db => getMongoUpdates(db, docName));
  t.assert(mergedUpdates.length === 1);

  await dbPersistence.flushDB();
};

var mongodb = /*#__PURE__*/Object.freeze({
  __proto__: null,
  testMongodbUpdateStorage: testMongodbUpdateStorage
});

if (environment_js.isNode) {
  t.runTests({
    mongodb
  }).then(success => {
    process.exit(success ? 0 : 1);
  });

}
//# sourceMappingURL=test.cjs.map
