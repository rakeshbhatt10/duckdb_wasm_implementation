import * as duckdb from '@duckdb/duckdb-wasm';
// import * as arrow from 'apache-arrow';   
import path from 'path';
import Worker from 'web-worker';
import {  table, loadArrow } from 'arquero';
import * as arrow from 'apache-arrow';


export class DBProvider {

    db: any
    
    constructor() {
    }

    public async initialize() {
        const MANUAL_BUNDLES: duckdb.DuckDBBundles = {
            mvp: {
                mainModule: path.resolve(__dirname, './libs/duckdb.wasm'),
                mainWorker: path.resolve(__dirname, './libs/duckdb-node.worker.cjs'),
            },
            eh: {
                mainModule: path.resolve(__dirname, './libs/duckdb-eh.wasm'),
                mainWorker: path.resolve(__dirname, './libs/duckdb-node-eh.worker.cjs'),
            },
        };
        // Select a bundle based on browser checks
        const bundle = await duckdb.selectBundle(MANUAL_BUNDLES);
        // Instantiate the asynchronus version of DuckDB-wasm
        const worker = new Worker(bundle.mainWorker!);
        const logger = new duckdb.ConsoleLogger();
        this.db = new duckdb.AsyncDuckDB(logger, worker);
        await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    }

    public async createTable(query) {
        const conn = await this.db.connect()
        // await conn.query(`drop table testInput`)
        await conn.query(query);
        await conn.close();
    }

    public async writeData(query) {
        const conn = await this.db.connect()
        await conn.query(query);
        await conn.close();
    }

    public async readData(tableName) {

        const conn = await this.db.connect();
        const data:arrow.Table = await conn.query(`
            SELECT * FROM ${tableName}
        `); 
        await conn.close();
        const aquarioInput = this.toAquarioObject(data.toArray());
        const dt = table(aquarioInput);
        return dt;
    }

    toAquarioObject(dataArray) {
        return dataArray.reduce((aquarioObject: object, input) => {
            Object.keys((input)).map((key) => {
                const keysArray = aquarioObject[key] ? aquarioObject[key]: [];
                keysArray.push(input[key]);
                aquarioObject[key] = keysArray;
            })
            return aquarioObject
        }, {});
    }
}