import { Activity, singleS3StreamInput } from 'aries-data';
import DropSchemaStrategy from './DropSchemaStrategy';
import AlterSchemaStrategy from './AlterSchemaStrategy';
import inferSchema from './util/inferSchema';
// import jsonSchemaGenerator from 'json-schema-generator';
import knex from 'knex';
import highland from 'highland';
import sqlserver from 'seriate';

export default class SQLServerSink extends Activity {
    static props = {
        name: require('../package.json').name,
        version: require('../package.json').version,
    };

    @singleS3StreamInput()
    async onTask(activityTask, config) {
        // Infer the incoming schema.
        const incomingSchema = await inferSchema(activityTask.input.file, config.schemaHint, config.json);

        // Create sqlserver connection object from configuration.
        const connection = Object.assign({ ssl: true }, config.connection);

        // Get a knex database connection for setting up schema/table.
        const knexdb = knex({ client: 'sqlserver', connection });

        await knexdb.schema.raw(`CREATE SCHEMA IF NOT EXISTS ${config.schema};`);

        // Produce a schema preparer.
        const schemaPreparer = config.drop
            ? new DropSchemaStrategy(knexdb)
            : new AlterSchemaStrategy(knexdb);

        // Prepare the schema for the INSERT.
        await schemaPreparer.prepare(config, incomingSchema);

        // Create and execute the INSERT command.
        await this.insertsqlserver(incomingSchema, activityTask.input.file, config, knexdb);

        await knexdb.destroy();
        this.log.info('Successfully destroyed connection pool.');
    }

    async insertsqlserver(incomingSchema, file, config, knexdb) {

        const sqlserverConnection = sqlserver.addConnection(config.connection);
        sqlserverConnection.connect(); //where does this method come from?

        // Create stream of batches
        const batchStream = highland(file).batch(1000);

        // Get column names
        const columnNames = incomingSchema.map(field => field.name);

        batchStream.on('data', (batch) => {
            // Get array of string objects
            const objectArray = batch.toString().split('\n');

            // Parse and append values to array
            const valueArr = objectArray.map((objString) => {
                const obj = JSON.parse(objString);
                return Object.values(obj);
            });

            // Insert into table
            sqlserverConnection.query('INSERT INTO ?? (??) VALUES ?', [config.table,columnNames, valueArr], (err, results) =>{
                if (err) { this.log.info('Did NOT successfully insert into sqlserver database: ' + err) };
                if (results) { this.log.info('Successfully ran INSERT to sqlserver database.') };
            });
        });
        sqlserverConnection.end();
    }
};
