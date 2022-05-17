import * as duckdb from '@duckdb/duckdb-wasm';
// import * as arrow from 'apache-arrow';   
import fs from 'fs';
import {getCreateTableSchema, generateInsertQuery} from './utils/helper';
import {DBProvider} from './utils/db'
import { table } from 'arquero';

const run = async () => {

    const rawJsonData = fs.readFileSync('vlx4qgVvxclk158d4ZdI.json');
    const data = JSON.parse(rawJsonData.toString());
    console.log("Data length : ", data.length);

    const source  = 'testInput';

    const {query, normalizedObjects} = getCreateTableSchema(data, source);
    
    const dbProvider = new DBProvider();
    
    // creating table
    await dbProvider.initialize();
    await dbProvider.createTable(query)

    // run insert query
    const insertQuery = generateInsertQuery(normalizedObjects.slice(0, 3), source);
    await dbProvider.writeData(insertQuery);
    
    // read table data 
    console.log(':============= Print table data =============')
    const tableData = await dbProvider.readData(source);
    tableData.select(['id', 'timestamp'])
    tableData.print();
}   



run();