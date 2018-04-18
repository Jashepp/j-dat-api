"use strict";

const fs = require('fs-extra');
const path = require('path');
const eventEmitter = require('events');
const _ = require('underscore');

const datDNS = require('dat-dns')(); // https://www.npmjs.com/package/dat-dns
const ram = require('random-access-memory');
const hyperDrive = require('hyperdrive'); // https://www.npmjs.com/package/hyperdrive
const hyperDiscovery = require('hyperdiscovery'); // https://www.npmjs.com/package/hyperdiscovery
const parseDatURL = require('parse-dat-url');

const datStore = require('dat-storage');
const raf = require('random-access-file');

class emptyWriteStream extends require('stream').Writable {
	constructor(...args){
		super(args);
	}
	_write(chunk,encoding,cb){
		setImmediate(cb);
	}
}

class jDatAPI extends eventEmitter {
	
	static get defaultConstructOptions(){
		return {
			metaPath: null,
			path: null,
			hyperUpload: false,
			hyperDownload: true,
			cacheSize: Number.POSITIVE_INFINITY,
			logPeers: false,
			waitForPeerConnect: false,
			maxConnections: 0,
			sparseContent: true,
			sparseMeta: true,
			whitelist: [],
			syncLatest: true,
			storeSecretsInAppData: false,
			utp: true,
			tcp: true
		};
	}
	
	constructor(datURL,options={}){
		super();
		_.extend(this,{
			constructOptions: _.extend({},jDatAPI.defaultConstructOptions,options,{ datURL }),
			hyperDrive: null,
			hyperDatSwarm: null,
			_archivePath: null,
			_initialising: false,
			_initialised: false,
			_datInfo: null,
			_datFileTree: null,
			_closing: false,
			_closed: false,
			_useDatStore: false,
			_archiveStorageDir: null,
			_archiveMetaDir: null
		});
		if(this.constructOptions.path!==null){
			this._useDatStore = true;
			if(this.constructOptions.metaPath===null) this.constructOptions.metaPath = path.join(this.constructOptions.path,'.dat');
		}
	}
	
	async initialise(){
		if(this._initialising || this._initialised) throw new Error("initialise() already invoked");
		this._initialising = true;
		this._archivePath = await jDatAPI.resolveDatURL(this.constructOptions.datURL);
		await this._createHyperDrive();
		this.emit('hyperDrive-ready');
		await this._createHyperDiscovery();
		this.emit('hyperDiscovery-ready');
		await this._fetchArchiveDatInfo();
		this._initialising = false;
		this._initialised = true;
		this.emit('ready');
		if(this._useDatStore && this._archivePath.uri!=='/') await this.download(this._archivePath.uri);
		return this;
	}
	
	async _createHyperDrive(){
		if(!this._initialising) throw new Error("initialise() not yet invoked");
		if(this.hyperDrive) throw new Error("_createHyperDrive() already invoked");
		var storage = this._archiveMetaDir = ram;
		if(this._useDatStore){
			// don't need to ensureDir, it's done via mkdirp in random-access-file in datStore
			this._archiveStorageDir = this.constructOptions.path;
			this._archiveMetaDir =  this.constructOptions.metaPath;
			var prefix = path.relative(this._archiveStorageDir,this._archiveMetaDir);
			if(prefix.length>0 && (prefix.substr(-1)!=='/' || prefix.substr(-1)!=='\\')) prefix += '/';
			var datStoreOpts = { prefix:prefix };
			if(!this.constructOptions.storeSecretsInAppData){
				datStoreOpts.secretDir = path.join(this._archiveMetaDir,'secrets');
			}
			storage = datStore(this._archiveStorageDir,datStoreOpts);
		}
		else {
			this._archiveMetaDir = this.constructOptions.metaPath;
			//await fs.ensureDir(this._archiveMetaDir); // done via mkdirp in random-access-file
			storage = {
				metadata: (name,opts)=>raf(path.join(this._archiveMetaDir, 'metadata.' + name)),
				content: (name,opts)=>raf(path.join(this._archiveMetaDir, 'content.' + name))
			};
		}
		// https://github.com/mafintosh/hyperdrive
		this.hyperDrive = hyperDrive(storage,this._archivePath.key,{
			sparse: !!this.constructOptions.sparseContent,
			sparseMetadata: !!this.constructOptions.sparseMeta,
			metadataStorageCacheSize: this.constructOptions.cacheSize,
			contentStorageCacheSize: this.constructOptions.cacheSize,
			treeCacheSize: this.constructOptions.cacheSize,
			latest: !!this.constructOptions.syncLatest
		});
		return new Promise((resolve,reject)=>this.hyperDrive.ready(()=>resolve()));
	}
	
	async _createHyperDiscovery(){
		if(!this.hyperDrive) throw new Error("_createHyperDrive() not yet invoked");
		if(this.hyperDatSwarm) throw new Error("_createHyperDiscovery() already invoked");
		// https://www.npmjs.com/package/hyperdiscovery
		// Basically: https://github.com/mafintosh/discovery-swarm
		// Leave, Join via key: this.hyperDrive.discoveryKey
		this.hyperDatSwarm = hyperDiscovery(this.hyperDrive,{
			upload: !!this.constructOptions.hyperUpload,
			download: !!this.constructOptions.hyperDownload,
			utp: !!this.constructOptions.utp,
			tcp: !!this.constructOptions.tcp,
			maxConnections: this.constructOptions.maxConnections,
			whitelist: this.constructOptions.whitelist
		});
		this.hyperDatSwarm.on('connection',(conn,info)=>{
			this.emit('peerCount',this.hyperDatSwarm.totalConnections);
			conn.on('close',()=>{
				this.emit('peerCount',this.hyperDatSwarm.totalConnections);
			});
		});
		// peer-specific events
		if(this.constructOptions.logPeers){
			this.hyperDatSwarm.on('peer',(peer)=>{
				console.log('Peer Discovered:',peer.host,peer.port);
			});
			this.hyperDatSwarm.on('peer-banned',(peer,details)=>{
				console.log('Peer Banned:',peer,{ details:details });
			});
			this.hyperDatSwarm.on('peer-rejected',(peer,details)=>{
				console.log('Peer Rejected:',peer,{ details:details });
			});
			this.hyperDatSwarm.on('drop',(peer)=>{
				console.log('Peer Dropped:',peer.host,peer.port);
			});
			this.hyperDatSwarm.on('connecting',(peer)=>{
				console.log('Peer Connecting:',peer.host,peer.port);
			});
			this.hyperDatSwarm.on('connect-failed',(peer)=>{
				console.log('Peer Connect-Failed:',peer.host,peer.port);
			});
			this.hyperDatSwarm.on('handshaking',(conn,info)=>{
				console.log('Peer Handshaking:',info.type,info.host,info.port);
			});
			this.hyperDatSwarm.on('connection',(conn,info)=>{
				console.log('Peer Connected:',info.type,info.host,info.port);
			});
			this.hyperDatSwarm.on('connection-closed',(conn,info)=>{
				console.log('Peer Connection-Closed:',info.type,info.host,info.port);
			});
			this.hyperDatSwarm.on('redundant-connection',(conn,info)=>{
				console.log('Peer Connection-Redundant:',info.type,info.host,info.port);
			});
		}
		if(this.constructOptions.waitForPeerConnect) return new Promise((resolve,reject)=>this.hyperDatSwarm.once('connection',()=>resolve()));
	}
	
	async _fetchArchiveDatInfo(){
		if(!this.hyperDatSwarm) throw new Error("_createHyperDiscovery() not yet invoked");
		if(this._datInfo) throw new Error("_fetchArchiveDatInfo() already invoked");
		return new Promise((resolve,reject)=>{
			this._datInfo = {};
			this.hyperDrive.readFile('dat.json','utf-8',(err,data)=>{ // Cheaty way to wait until metadata is downloaded, even if the readFile path doesn't exist
				if(!err && data) try{
					this._datInfo = JSON.parse(data);
				}catch(err2){}
				resolve();
			});
		});
	}
	
	async _fetchArchiveFileTree(){
		if(!this.hyperDatSwarm) throw new Error("_createHyperDiscovery() not yet invoked");
		if(!this._datInfo) throw new Error("_fetchArchiveDatInfo() not yet invoked");
		if(this._datFileTree) throw new Error("_fetchArchiveFileTree() already invoked");
		this._datFileTree = await jDatAPI.fetchArchiveFileTree(this.hyperDrive,'/');
		return this._datFileTree;
	}
	
	async close(){
		if(this._closing || this._closed) throw new Error("close() already invoked");
		this._closing = true;
		this.hyperDatSwarm.close();
		return new Promise((resolve,reject)=>this.hyperDrive.close(resolve))
		.then(()=>{
			this._closing = false;
			this._closed = true;
			this.hyperDatSwarm.destroy();
		});
	}
	
	get archiveKey(){
		if(!this._archivePath) return null;
		return this._archivePath.key;
	}
	
	get archiveInfo(){
		if(!this._datInfo) return null;
		return this._datInfo;
	}
	
	get peerCount(){
		if(!this.hyperDatSwarm) return null;
		return this.hyperDatSwarm.totalConnections;
	}
	
	async archiveFileTree(){
		if(!this._initialised) throw new Error("initialise() not yet invoked");
		if(!this._datFileTree) await this._fetchArchiveFileTree(); // https://github.com/mafintosh/hyperdrive/issues/156
		return this._datFileTree;
	}
	
	isReady(){
		return (this.hyperDrive && this.hyperDatSwarm && this._datInfo);
	}
	
	removeMetadataDirectory(){
		if(!this.isReady()) return Promise.reject("dat archive not yet ready");
		if(this._closing) return Promise.reject("close() still running");
		if(!this._closed) return Promise.reject("close() not yet invoked");
		if(this._archiveMetaDir===ram) return Promise.reject("Directory is set to RAM");
		if(this._archiveMetaDir===null) return Promise.reject("Metadata directory null?");
		if(this._useDatStore) return Promise.reject("sorry, datStore has some metadata files locked");
		return fs.remove(this._archiveMetaDir);
	}
	
	readFile(file,encoding='utf-8'){
		if(!this.isReady()) return Promise.reject("dat archive not yet ready");
		return new Promise((resolve,reject)=>{
			this.hyperDrive.readFile(file,encoding,(err,data)=>{
				if(err) reject(err);
				else resolve(data);
			});
		});
	}
	
	readFileStream(file,options={}){
		return this.hyperDrive.createReadStream(file,options);
	}
	
	readHistoryStream(){
		return this.hyperDrive.history();
	}
	
	download(remote,local){
		if(remote===void 0 || remote==='/' || remote==='.' || remote==='/.') return this.downloadDir('/',local);
		if(!this.isReady()) return Promise.reject("dat archive not yet ready");
		return new Promise((resolve,reject)=>{
			this.hyperDrive.stat(remote,(err,stat)=>{
				if(err || !stat) return reject(err);
				else if(stat.isDirectory()) resolve(this.downloadDir(remote,local));
				else if(stat.isFile()) resolve(this.downloadFile(remote,local));
			});
		});
	}
	
	downloadFile(remoteFile,localFile){
		if(!this.isReady()) return Promise.reject("dat archive not yet ready");
		if(!this._useDatStore && localFile===void 0) return Promise.reject("not using datStore, localFile must be specified");
		var usingDatStore = (this._useDatStore && localFile===void 0);
		return new Promise((resolve,reject)=>{
			if(usingDatStore) var local = new emptyWriteStream();
			else var local = require("fs").createWriteStream(localFile);
			local.once('finish',resolve);
			local.once('error',reject);
			var remote = this.hyperDrive.createReadStream(remoteFile);
			remote.once('error',reject);
			remote.pipe(local);
		});
	}
	
	async downloadDir(remoteDir,localDir){
		if(!this._useDatStore && localDir===void 0) return Promise.reject("not using datStore, localDir must be specified");
		var usingDatStore = (this._useDatStore && localDir===void 0);
		if(!usingDatStore) await fs.ensureDir(localDir);
		return new Promise((resolve,reject)=>{
			this.hyperDrive.readdir(remoteDir,(err,items)=>{
				var promiseArr = [];
				items.forEach((item)=>{
					if(item==='.' || item==='..') return;
					let remoteItemPath = path.posix.join(remoteDir,item);
					let localItemPath = usingDatStore ? void 0 : path.posix.join(localDir,item);
					promiseArr.push(this.download(remoteItemPath,localItemPath));
				});
				resolve(Promise.all(promiseArr));
			});
		});
	}
	
	get version(){
		return this.hyperDrive.version;
	}
	
	static async resolveDatURL(datURL){
		let result = parseDatURL(datURL);
		let { protocol, host:key, path:uri, version } = result;
		/*
		var urlShort = datURL;
		if(datURL.substr(6).indexOf('/')===-1) datURL += '/';
		var pos = datURL.substr(6).indexOf('/')+6;
		if(datURL.length<=14) urlShort = datURL;
		else urlShort = datURL.substr(0,12)+'..'+datURL.substr(pos-2);
		let [url,protocol,key,uri] = (/^(.*?\:\/\/)(.*?)(\/.*)$/).exec(datURL);
		*/
		if(key.indexOf('.')!==-1){ // eg, if /.well-known/dat exists at https://domain/ with response: dat://000000.../ as first line, ttl=3600 as second line
			return datDNS.resolveName(key,{ ignoreCache:true })
			.then((key)=>({ url:datURL, protocol, key, uri, version }));
		} else {
			return { url:datURL, protocol, key, uri, version };
		}
	};
	
	static fetchArchiveFileTree(archive,fsPath){
		return new Promise((resolve,reject)=>{
			archive.readdir(fsPath,(err,files)=>{ // This only wants to work once metadata is downloaded
				if(err) return reject(err);
				let tree = {};
				let promiseArr = [];
				for(var i=0,l=files.length; i<l; i++){
					let file = files[i];
					if(file==='.' || file==='..') continue;
					let filePath = path.posix.join(fsPath,file);
					let fileObj = tree[file] = { path:filePath, type:null, stat:null };
					promiseArr.push(new Promise((resolve2,reject2)=>{
						archive.stat(filePath,(err,stat)=>{
							if(err) return reject2(err);
							fileObj.stat = stat;
							if(fileObj.stat.isDirectory()) fileObj.type = 2;
							else if(fileObj.stat.isFile()) fileObj.type = 1;
							if(fileObj.type===2){
								fileObj.fileTree = null;
								jDatAPI.fetchArchiveFileTree(archive,filePath)
								.then((tree)=>{
									fileObj.fileTree = tree;
								})
								.then(resolve2,reject2);
							} else {
								resolve2();
							}
						})
					}));
				}
				if(promiseArr.length>0){
					Promise.all(promiseArr)
					.then(()=>tree)
					.then(resolve)
					.catch(reject);
				} else {
					resolve(tree);
				}
			});
		});
	};
	
}

module.exports = jDatAPI;
