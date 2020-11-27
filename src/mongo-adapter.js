const { MongoClient } = require('mongodb')

export class MongoAdapter {
  constructor (location, dbName, collection) {
    this.location = location
    this.dbName = dbName
    this.collection = collection || 'yjs-writings'
    this.db = null
    this.open()
  }

  open () {
    const mongojsDb = new MongoClient(this.location, {useUnifiedTopology: true})

    const connect = async (dbName) => {
      try{
        await mongojsDb.connect()

        const db = mongojsDb.db(dbName)
        this.db = db
        console.log("=> Connected to Y-MongoDB")
      }finally{
        await mongojsDb.close()
      }
    }

    connect(this.dbName)
  }

  async get (query) {
    return await this.db.collection(this.collection).findOne(query)
  }

  async put (values) {
    if (!values.docName && !values.version && !values.value) { throw new Error('Document and version must be provided') }

    return await this.db.collection(this.collection).save(values)
  }

  async del (query) {
    const bulk = await this.db.collection(this.collection).initializeOrderedBulkOp()
    await bulk.find(query).remove()
    return await bulk.execute()
  }

  async readAsCursor (query, opts = {}) {
    return await this.db.collection(this.collection).find(query).limit(opts.limit).sort({clock: -1}).toArray()
  }

  async close () {
    return await this.db.close()
  }

  async flush () {
    await this.db.dropDatabase()
    await this.db.close()
  }
}
