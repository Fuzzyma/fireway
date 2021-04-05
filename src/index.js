const path = require('path');
const util = require('util');
const os = require('os');
const fs = require('fs');
const md5 = require('md5');
const admin = require('firebase-admin');
const {Firestore, WriteBatch, FieldValue, FieldPath, Timestamp} = require('@google-cloud/firestore');
const semver = require('semver');

const readFile = util.promisify(fs.readFile);
const readdir = util.promisify(fs.readdir);
const stat = util.promisify(fs.stat);
const exists = util.promisify(fs.exists);

// Track stats and dryrun setting so we only proxy once.
// Multiple proxies would create a memory leak.
const statsMap = new Map();

let proxied = false;
function proxyWritableMethods() {
	// Only proxy once
	if (proxied) return;
	else proxied = true;

	const ogCommit = WriteBatch.prototype._commit;
	WriteBatch.prototype._commit = async function() {
		// Empty the queue
		while (this._fireway_queue?.length) {
			this._fireway_queue.shift()();
		}
		for (const [stats, {dryrun}] of statsMap.entries()) {
			if (this._firestore._fireway_stats === stats) {
				if (dryrun) return [];
			}
		}
		return ogCommit.apply(this, Array.from(arguments));
	};

	function mitm(obj, key, fn) {
		const original = obj[key];
		obj[key] = function() {
			const args = [...arguments];
			for (const [stats, {log}] of statsMap.entries()) {
				if (this._firestore._fireway_stats === stats) {
					this._fireway_queue = this._fireway_queue || [];
					this._fireway_queue.push(() => {
						fn.call(this, args, (stats.frozen ? {} : stats), log);
					});
				}
			}
			return original.apply(this, args);
		}
	}

	// Add logs for each WriteBatch item
	mitm(WriteBatch.prototype, 'create', ([_, doc], stats, log) => {
		stats.created += 1;
		log('Creating', JSON.stringify(doc));
	});

	mitm(WriteBatch.prototype, 'set', ([ref, doc, opts = {}], stats, log) => {
		stats.set += 1;
		log(opts.merge ? 'Merging' : 'Setting', ref.path, JSON.stringify(doc));
	});

	mitm(WriteBatch.prototype, 'update', ([ref, doc], stats, log) => {
		stats.updated += 1;
		log('Updating', ref.path, JSON.stringify(doc));
	});

	mitm(WriteBatch.prototype, 'delete', ([ref], stats, log) => {
		stats.deleted += 1;
		log('Deleting', ref.path);
	});
}

async function migrate({path: dir, projectId, storageBucket, dryrun, app, debug = false} = {}) {
	const log = function() {
		return debug && console.log.apply(console, arguments);
	}

	const stats = {
		scannedFiles: 0,
		executedFiles: 0,
		created: 0,
		set: 0,
		updated: 0,
		deleted: 0,
		added: 0
	};

	// Get all the scripts
	if (!path.isAbsolute(dir)) {
		dir = path.join(process.cwd(), dir);
	}

	if (!(await exists(dir))) {
		throw new Error(`No directory at ${dir}`);
	}

	const filenames = [];
	for (const file of await readdir(dir)) {
		if (!(await stat(path.join(dir, file))).isDirectory()) {
			filenames.push(file);
		}
	}

	// Parse the version numbers from the script filenames
	const versionToFile = new Map();
	let files = filenames.map(filename => {
		// Skip files that start with a dot
		if (filename[0] === '.') return;
		
		const [filenameVersion, description] = filename.split('__');
		const coerced = semver.coerce(filenameVersion);

		if (!coerced) {
			if (description) {
				// If there's a description, we assume you meant to use this file
				log(`WARNING: ${filename} doesn't have a valid semver version`);
			}
			return null;
		}

		// If there's a version, but no description, we have an issue
		if (!description) {
			throw new Error(`This filename doesn't match the required format: ${filename}`);
		}

		const {version} = coerced;

		const existingFile = versionToFile.get(version);
		if (existingFile) {
			throw new Error(`Both ${filename} and ${existingFile} have the same version`);
		}
		versionToFile.set(version, filename);

		return {
			filename,
			path: path.join(dir, filename),
			version,
			description: path.basename(description, '.js')
		};
	}).filter(Boolean);

	stats.scannedFiles = files.length;
	log(`Found ${stats.scannedFiles} migration files`);

	// Find the files after the latest migration number
	statsMap.set(stats, {dryrun, log});
	dryrun && log('Making firestore read-only');
	proxyWritableMethods();

	if (!storageBucket && projectId) {
		storageBucket = `${projectId}.appspot.com`;
	}
	
	const providedApp = app;
	if (!app) {
		app = admin.initializeApp({
			projectId,
			storageBucket
		});
	}

	// Use Firestore directly so we can mock for dryruns
	const firestore = new Firestore({projectId});
	firestore._fireway_stats = stats;

	const collection = firestore.collection('fireway');

	// Get the latest migration
	const result = await collection
		.orderBy('installed_rank', 'desc')
		.limit(1)
		.get();
	const [latestDoc] = result.docs;
	const latest = latestDoc && latestDoc.data();

	if (latest && !latest.success) {
		throw new Error(`Migration to version ${latest.version} using ${latest.script} failed! Please restore backups and roll back database and code!`);
	}

	let installed_rank;
	if (latest) {
		files = files.filter(file => semver.gt(file.version, latest.version));
		installed_rank = latest.installed_rank;
	} else {
		installed_rank = -1;
	}

	// Sort them by semver
	files.sort((f1, f2) => semver.compare(f1.version, f2.version));

	log(`Executing ${files.length} migration files`);

	// Execute them in order
	for (const file of files) {
		stats.executedFiles += 1;
		log('Running', file.filename);
		
		let migration;
		try {
			migration = require(file.path);
		} catch (e) {
			log(e);
			throw e;
		}

		const start = new Date();
		let success, finish;
		try {
			await migration.migrate({app, firestore, FieldValue, FieldPath, Timestamp, dryrun});
			success = true;
		} catch(e) {
			log(`Error in ${file.filename}`, e);
			success = false;
		} finally {
			finish = new Date();
		}

		// Upload the results
		log(`Uploading the results for ${file.filename}`);

		// Freeze stat tracking
		stats.frozen = true;

		installed_rank += 1;
		const id = `${installed_rank}-${file.version}-${file.description}`;
		await collection.doc(id).set({
			installed_rank,
			description: file.description,
			version: file.version,
			script: file.filename,
			type: 'js',
			checksum: md5(await readFile(file.path)),
			installed_by: os.userInfo().username,
			installed_on: start,
			execution_time: finish - start,
			success
		});

		// Unfreeze stat tracking
		delete stats.frozen;

		if (!success) {
			throw new Error('Stopped at first failure');
		}
	}

	// Ensure firebase terminates
	if (!providedApp) {
		app.delete();
	}

	const {scannedFiles, executedFiles, added, created, updated, set, deleted} = stats;
	log('Finished all firestore migrations');
	log(`Files scanned:${scannedFiles} executed:${executedFiles}`);
	log(`Docs added:${added} created:${created} updated:${updated} set:${set} deleted:${deleted}`);

	statsMap.delete(stats);

	return stats;
}

module.exports = {migrate};
