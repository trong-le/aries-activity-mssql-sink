import { Activity, singleS3StreamInput } from 'aries-data';
import DropSchemaStrategy from './DropSchemaStrategy';
import AlterSchemaStrategy from './AlterSchemaStrategy';
import inferSchema from './util/inferSchema';
// import jsonSchemaGenerator from 'json-schema-generator';
import knex from 'knex';
import highland from 'highland';
import sqlserver from 'seriate';
import pkgJson from '../package.json';
import streamToMSSql from './MSSQLWriteStream';

export default class SQLServerSink extends Activity {
    static props = {
        name: pkgJson.name,
        version: pkgJson.version,
    };

    @singleS3StreamInput()
    async onTask(activityTask, config) {

        await this.writeFromStream(activityTask.input.file, config);


        // Create duplicate stream for inferSchema and insertion values
        const aStream = activityTask.input.file.fork();
        const bStream = activityTask.input.file.observe();

        // Infer the incoming schema.
        aStream.resume();
        const incomingSchema = await inferSchema(aStream, config.schemaHint, config.json);

        // Create sqlserver connection object from configuration.
        const connection = Object.assign({ ssl: true }, config.connection);

        // Get a knex database connection for setting up schema/table.
        const knexdb = knex({ client: 'sqlserver', connection });

        // Produce a schema preparer.
        const schemaPreparer = config.drop
            ? new DropSchemaStrategy(knexdb)
            : new AlterSchemaStrategy(knexdb);

        // Prepare the schema for the INSERT.
        await schemaPreparer.prepare(config, incomingSchema);

        // Create and execute the INSERT command.
        const outStream = this.insertSqlserver(incomingSchema, bStream, config);
        await new Promise((resolve, reject) => {
            outStream.toCallback((err, data) => {
                err ? reject(err) : resolve(data);
            });
        });

        await knexdb.destroy();
        this.log.info('Successfully destroyed connection pool.');
    }

    // pipe all incoming data into MSSQLWriteStream
    writeFromStream(stream, options) {
        const writeStream = new streamToMSSql(optins);
        stream.pipe(JSON.parse()).pipe(writeStream);
    }

    insertSqlserver(incomingSchema, stream, config) {
        const mySqlConnection = sqlserver.addConnection(config.connection);
        // mySqlConnection.connect();
        this.log.debug('Created MySQL connection');

        const columnNames = incomingSchema.map(field => field.name).join(',');

        // Create stream of batches
        return highland(stream)
            .batch(1000)
            .consume((err, data, push, next) => {
                if (err) {
                    push(err);
                    next();
                } else if (data === highland.nil) {
                    push(null, data);
                } else {
                    const objectArray = data.toString().split('\n');

                    // Format batch insert values
                    const values = objectArray.map((objString) => { 
                        const arr = Object.values(JSON.parse(objString));
                        // Check for string value
                        let valueString = '(';
                            arr.forEach((value, i) => {
                            // Need to add quotes/commas for string values and value separation
                            typeof value === 'string' ? valueString += '\'' + value + '\',' : valueString += value + ',';
                        });
                        // Remove last comma
                        valueString = valueString.slice(0, -1) + ')';
                    return valueString;
                });
                    const joinedValues = values.join(',');

                    // Create query with inserted values
                    const query = `INSERT INTO ${config.table} (${columnNames}) VALUES ${joinedValues}`;

                    // Run query
                    mySqlConnection.query(query, (err, results) =>{
                        if (err) { 
                            this.log.debug('Did NOT successfully insert into MySQL database: ' + err);
                            push(err);
                        }
                        if (results) { this.log.debug('Successfully insert into MySQL database') }
                        next();
                    });
                }
            })
        }
};
