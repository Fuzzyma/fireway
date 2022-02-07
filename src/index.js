const path = require('path')
const { EventEmitter } = require('events')
const util = require('util')
const fs = require('fs')
const admin = require('firebase-admin')
const { Firestore, WriteBatch, CollectionReference, FieldValue, FieldPath, Timestamp } = require('@google-cloud/firestore')
const semver = require('semver')
const asyncHooks = require('async_hooks')
const callsites = require('callsites')

const readdir = util.promisify(fs.readdir)
const stat = util.promisify(fs.stat)
const access = util.promisify(fs.access)

// Track stats and dryrun setting so we only proxy once.
// Multiple proxies would create a memory leak.
const statsMap = new Map()

let proxied = false
function proxyWritableMethods () {
  // Only proxy once
  if (proxied) return
  else proxied = true

  const ogCommit = WriteBatch.prototype._commit
  WriteBatch.prototype._commit = async function () {
    // Empty the queue
    while (this._fireway_queue && this._fireway_queue.length) {
      this._fireway_queue.shift()()
    }
    for (const [stats, { dryrun }] of statsMap.entries()) {
      if (this._firestore._fireway_stats === stats) {
        if (dryrun) return []
      }
    }
    return ogCommit.apply(this, Array.from(arguments))
  }

  const skipWriteBatch = Symbol('Skip the WriteBatch proxy')

  function mitm (obj, key, fn) {
    const original = obj[key]
    obj[key] = function () {
      const args = [...arguments]
      for (const [stats, { log }] of statsMap.entries()) {
        if (this._firestore._fireway_stats === stats) {
          // If this is a batch
          if (this instanceof WriteBatch) {
            const [, doc] = args
            if (doc && doc[skipWriteBatch]) {
              delete doc[skipWriteBatch]
            } else {
              this._fireway_queue = this._fireway_queue || []
              this._fireway_queue.push(() => {
                fn.call(this, args, (stats.frozen ? {} : stats), log)
              })
            }
          } else {
            fn.call(this, args, (stats.frozen ? {} : stats), log)
          }
        }
      }
      return original.apply(this, args)
    }
  }

  // Add logs for each WriteBatch item
  mitm(WriteBatch.prototype, 'create', ([_, doc], stats, log) => {
    stats.created += 1
    log('Creating', JSON.stringify(doc))
  })

  mitm(WriteBatch.prototype, 'set', ([ref, doc, opts = {}], stats, log) => {
    stats.set += 1
    log(opts.merge ? 'Merging' : 'Setting', ref.path, JSON.stringify(doc))
  })

  mitm(WriteBatch.prototype, 'update', ([ref, doc], stats, log) => {
    stats.updated += 1
    log('Updating', ref.path, JSON.stringify(doc))
  })

  mitm(WriteBatch.prototype, 'delete', ([ref], stats, log) => {
    stats.deleted += 1
    log('Deleting', ref.path)
  })

  mitm(CollectionReference.prototype, 'add', ([doc], stats, log) => {
    doc[skipWriteBatch] = true
    stats.added += 1
    log('Adding', JSON.stringify(doc))
  })
}

const dontTrack = Symbol('Skip async tracking to short circuit')
async function trackAsync ({ log, file, forceWait }, fn) {
  // Track filenames for async handles
  const activeHandles = new Map()
  const emitter = new EventEmitter()
  function deleteHandle (id) {
    if (activeHandles.has(id)) {
      activeHandles.delete(id)
      emitter.emit('deleted', id)
    }
  }
  function waitForDeleted () {
    return new Promise(resolve => emitter.once('deleted', () => resolve()))
  }
  const hook = asyncHooks.createHook({
    init (asyncId) {
      for (const call of callsites()) {
        // Prevent infinite loops
        const fn = call.getFunction()
        if (fn && fn[dontTrack]) {
          return
        }

        const name = call.getFileName()
        if (
          !name ||
          name === __filename ||
          name.startsWith('internal/') ||
          name.startsWith('timers.js')
        ) continue

        if (name === file.path) {
          const filename = call.getFileName()
          const lineNumber = call.getLineNumber()
          const columnNumber = call.getColumnNumber()
          activeHandles.set(asyncId, `${filename}:${lineNumber}:${columnNumber}`)
          break
        }
      }
    },
    before: deleteHandle,
    after: deleteHandle,
    promiseResolve: deleteHandle
  }).enable()

  let logged
  async function handleCheck () {
    while (activeHandles.size) {
      if (forceWait) {
        // NOTE: Attempting to add a timeout requires
        // shutting down the entire process cleanly.
        // If someone decides not to return proper
        // Promises, and provides --forceWait, long
        // waits are expected.
        if (!logged) {
          log('Waiting for async calls to resolve')
          logged = true
        }
        await waitForDeleted()
      } else {
        // This always logs in Node <12
        const nodeVersion = semver.coerce(process.versions.node)
        if (nodeVersion.major >= 12) {
          console.warn(
            'WARNING: fireway detected open async calls. Use --forceWait if you want to wait:',
            Array.from(activeHandles.values())
          )
        }
        break
      }
    }
  }

  let rejection
  const unhandled = reason => { rejection = reason }
  process.once('unhandledRejection', unhandled)
  process.once('uncaughtException', unhandled)

  try {
    const res = await fn()
    await handleCheck()

    // Wait a tick or so for the unhandledRejection
    await new Promise(resolve => setTimeout(() => resolve(), 1))

    process.removeAllListeners('unhandledRejection')
    process.removeAllListeners('uncaughtException')
    if (rejection) {
      log(`Error in ${file.filename}`, rejection)
      return false
    }
    return res
  } catch (e) {
    log(e)
    return false
  } finally {
    hook.disable()
  }
}
trackAsync[dontTrack] = true

async function migrate ({ path: dir, projectId, storageBucket, dryrun, app, debug = false, require: req, forceWait = false, stats: showstats = false } = {}) {
  if (req) {
    try {
      require(req)
    } catch (e) {
      console.error(e)
      throw new Error(`Trouble executing require('${req}');`)
    }
  }

  const log = function () {
    return debug && console.log.apply(console, arguments)
  }

  const stats = {
    scannedFiles: 0,
    executedFiles: 0,
    created: 0,
    set: 0,
    updated: 0,
    deleted: 0,
    added: 0
  }

  // Get all the scripts
  if (!path.isAbsolute(dir)) {
    dir = path.join(process.cwd(), dir)
  }

  try {
    await access(dir, fs.constants.F_OK)
  } catch {
    throw new Error(`No directory at ${dir}`)
  }

  const filenames = []
  for (const file of await readdir(dir)) {
    if (!(await stat(path.join(dir, file))).isDirectory()) {
      filenames.push(file)
    }
  }

  // Parse the version numbers from the script filenames
  const files = filenames.map(filename => {
    // Skip files that start with a dot
    if (filename[0] === '.') return null

    // Expecting a filename like: [YYYY-MM-DD]_[HH-mm-ss]_[name].[jt]s
    const match = filename.match(/^(\d{4}-\d{2}-\d{2})-(\d{2}-\d{2}-\d{2})_(.+).[jt]s$/)
    if (!match) {
      throw new Error(`Invalid filename: ${filename}. Expecting: [YYYY-MM-DD]-[HH-mm-ss]_[name].[jt]s`)
    }

    return {
      filename,
      path: path.join(dir, filename)
    }
  }).filter(Boolean).sort((a, b) => a.filename < b.filename ? -1 : 1)

  stats.scannedFiles = files.length
  log(`Found ${stats.scannedFiles} migration files`)

  // Find the files after the latest migration number
  statsMap.set(stats, { dryrun, log })
  dryrun && log('Making firestore read-only')
  proxyWritableMethods()

  if (!storageBucket && projectId) {
    storageBucket = `${projectId}.appspot.com`
  }

  const providedApp = app
  if (!app) {
    app = admin.initializeApp({
      projectId,
      storageBucket
    })
  }

  // Use Firestore directly so we can mock for dryruns
  const firestore = new Firestore({ projectId })
  firestore._fireway_stats = stats

  const collection = firestore.collection('fireway')

  const migrationDocs = await collection.orderBy('filename').get()

  let doc
  while ((doc = migrationDocs.docs.shift())) {
    if (doc.filename === files[0].filename) {
      files.shift()
      continue
    } else {
      const index = files.findIndex(file => file.filename === doc.filename)
      if (index > -1) {
        files.splice(index, 1)
        console.warn('Migrations were not run in order. Please make sure that migration succeeded.')
      }
      break
    }
  }

  if (migrationDocs.length) {
    console.warn('The following migrations existed before but a corresponding file wasnt found:', migrationDocs.map(doc => doc.filename))
  }

  log(`Executing ${files.length} migration files`)

  // Execute them in order
  for (const file of files) {
    stats.executedFiles += 1
    log('Running', file.filename)

    let migration
    try {
      migration = require(file.path)
    } catch (e) {
      log(e)
      throw e
    }

    let start, finish
    const success = await trackAsync({ log, file, forceWait }, async () => {
      start = new Date()
      try {
        await migration.migrate({ app, firestore, FieldValue, FieldPath, Timestamp, dryrun })
        return true
      } catch (e) {
        log(`Error in ${file.filename}`, e)
        return false
      } finally {
        finish = new Date()
      }
    })

    if (!success) {
      throw new Error('Migration of ' + file.filename + ' failed. Roll back database and try again.')
    }

    // Upload the results
    log(`Uploading the results for ${file.filename}`)

    // Freeze stat tracking
    stats.frozen = true
    await collection.doc(file.filename).set({
      filename: file.filename,
      installed_on: start,
      execution_time: finish - start
    })

    // Unfreeze stat tracking
    delete stats.frozen
  }

  // Ensure firebase terminates
  if (!providedApp) {
    app.delete()
  }

  const { scannedFiles, executedFiles, added, created, updated, set, deleted } = stats
  log('Finished all firestore migrations')
  log(`Files scanned:${scannedFiles} executed:${executedFiles}`)
  showstats && console.log(`Docs added:${added} created:${created} updated:${updated} set:${set} deleted:${deleted}`)

  statsMap.delete(stats)

  return stats
}

function writeFileSyncRecursive (filename, content, charset) {
  const folders = filename.split(path.sep).slice(0, -1)
  if (folders.length) {
    // create folder path if it doesn't exist
    folders.reduce((last, folder) => {
      const folderPath = last ? last + path.sep + folder : folder
      if (!fs.existsSync(folderPath)) {
        fs.mkdirSync(folderPath)
      }
      return folderPath
    })
  }
  fs.writeFileSync(filename, content, charset)
}

// Create filename of the form: [YYYY-MM-DD]-[HH-mm-ss]_[name].ts
function getFileName (name, ext = 'ts') {
  const d = new Date()
  const filename = `${d.getUTCFullYear()}-${(d.getUTCMonth() + 1).toString().padStart(2, '0')}-${d.getUTCDate().toString().padStart(2, '0')}-${d.getUTCHours().toString().padStart(2, '0')}-${d.getUTCMinutes().toString().padStart(2, '0')}-${d.getUTCSeconds().toString().padStart(2, '0')}_${name}${ext ? '.' : ''}${ext}`
  return filename
}

async function addMigration (name, { path: dir = '' } = {}) {
  const filename = getFileName(name)
  const filepath = path.resolve(dir, filename)

  writeFileSyncRecursive(filepath, `import { MigrateOptions } from "fireway";

module.exports.migrate = async (opts: MigrateOptions) => {

}
`, 'utf8')
}

// Converts old file naming scheme to new one
async function convert ({ path: dir = '' } = {}) {
  // Read the files in the directory
  const filenames = await readdir(dir)

  // Split by __ to get semver and name
  filenames.forEach(filename => {
    if (filename[0] === '.') return null
    const newFileName = getFileName(filename, '')

    // rename the file
    fs.renameSync(path.join(dir, filename), path.join(dir, newFileName))
  })
}

module.exports = { migrate, addMigration, convert }
