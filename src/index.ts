import * as duckdb from '@duckdb/duckdb-wasm';
// import * as arrow from 'apache-arrow';   
import path from 'path';
import Worker from 'web-worker';

const run = async () => {

    console.log("Hello");

    const MANUAL_BUNDLES: duckdb.DuckDBBundles = {
        mvp: {
            mainModule: path.resolve(__dirname, './duckdb.wasm'),
            mainWorker: path.resolve(__dirname, './duckdb-node.worker.cjs'),
        },
        eh: {
            mainModule: path.resolve(__dirname, './duckdb-eh.wasm'),
            mainWorker: path.resolve(__dirname, './duckdb-node-eh.worker.cjs'),
        },
    };
    // Select a bundle based on browser checks
    const bundle = await duckdb.selectBundle(MANUAL_BUNDLES);
    // Instantiate the asynchronus version of DuckDB-wasm
    const worker = new Worker(bundle.mainWorker!);
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    const c = await db.connect();
    
    // await db.registerFileText(
    //     'rows.json',
    //     `[
    //     { "col1": 1, "col2": "foo" },
    //     { "col1": 2, "col2": "bar" },
    // ]`,
    // );

    // await c.insertJSONFromPath('rows.json', { name: 'rows' });

    // await c.insertArrowTable(existingTable, { name: 'arrow_table' });
    // await c.query(`INSERT INTO existing_table
    // VALUES (1, "foo"), (2, "bar")`);
    // await c.query(`INSERT INTO existing_table
    // VALUES (1, "foo"), (2, "bar")`);

    await c.query(`CREATE TABLE existing_table(foo INTEGER, bar INTEGER);`);


    await c.query(`INSERT INTO existing_table
    VALUES (1, 2)`);

    await c.close();

    const conn = await db.connect();

    const data = await conn.query(`
        SELECT * FROM existing_table
    `); 

    // console.log(data.batches)
    for (const batch of data.batches) {
        const filedNames = batch.schema.fields.map((field) => field.name);
        console.log('Columns : ', filedNames);

        console.log('Batch length : ', batch.data.length);
        // console.log(batch.data.children);
        console.log('Row Values')
        for (const value of batch.data.children) {
            
            console.log('Column Value : ', value.values);
        }
        // console.log(batch);
    }

//     for await (const  batch of await conn.send(`
//     SELECT * FROM existing_table
// `)) {
//         console.log('Batch: ', batch.data);
//     }
    

    // Close the connection to release memory
    await conn.close();
}


run();