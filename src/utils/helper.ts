import knex from 'knex';

export function flatJSONArray(inputs) {

    const flatten = (obj, prefix:Array<string> = [], current = {}) => {
        if (typeof(obj) === 'object' && obj !== null) {
          for (let key of Object.keys(obj)) {
            
            // console.log('Prefixes : ', prefix, key);
            flatten(obj[key], prefix.concat([key]), current);
          }
        } else {
          current[prefix.join('_').toLowerCase()] = obj
        }
        return current
    }

    return inputs.map((input) => {
        // console.log("Running for ", input);
        return flatten(input)
    });
}

function prepareIdealObject(keys, inputs) {
    return keys.reduce((finalObject: any, key) => {
        const filteredInput = inputs.filter((input) => input[key])[0];
        finalObject[key] = filteredInput[key];
        return finalObject;
    }, {});
}



function createSchema(keys, jsonObject, tableName) {
    const knexClient = knex({
        client: 'pg'
    });
    return knexClient.schema.createTable(
        tableName,
        (table) => {
            for (const key of keys) {
                if(typeof jsonObject[key] === 'boolean') {
                    table.boolean(key);
                } else if(typeof jsonObject[key] == 'string') {
                    table.string(key);
                } else if(typeof jsonObject[key] == 'number') {
                    table.bigInteger(key);
                } else {
                    table.string(key);
                }
            }
            // table.timestamps();
        }
    )
}

export function getCreateTableSchema(inputs, source) {
    /* 
        Converting json array to flat json structure
        Current json input object schema can be nested we need
        to flat it to make sure we have single flat json array structure
    */
    const flatJSONS = flatJSONArray(inputs);

    /*
        Considering a case when all objects dosen't have all keys in that 
        case prepare all keys to which can be columns in in table
    */
    const allKeys = flatJSONS.reduce((keys: Array<string>, jsonInput) => {
        
        Object.keys(jsonInput).map((key) => {
            if (keys.indexOf(key) < 0) {
                keys.push(key);
            };
        });
        return keys;
    },  []);

    /* 
        This is to check datatype of all key values and prepare data types for 
        each input key value
    */
    const idealObject = prepareIdealObject(allKeys, flatJSONS);
    const createTableQuery = createSchema(allKeys, idealObject, source);

    return {query: createTableQuery.toQuery(), normalizedObjects: flatJSONS };
}


export function generateInsertQuery(input, source) {
    const knexClient = knex({
        client: 'pg'
    });
    return knexClient(source).insert(input).toQuery();
}