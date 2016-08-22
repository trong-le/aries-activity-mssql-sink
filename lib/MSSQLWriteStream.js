import { logger } from 'aries-data';
import { Writable } from 'stream';
import { Connection, Request } from 'mssql';
import DropSchemaStrategy from './DropSchemaStrategy';
import AlterSchemaStrategy from './AlterSchemaStrategy';
import inferSchema from './util/inferSchema';
// import jsonSchemaGenerator from 'json-schema-generator';
import knex from 'knex';
import highland from 'highland';
import sqlserver from 'seriate';

@logger()
export default class MSSQLWriteStream extends Writable {

    constructor(config) {
        super({
            // objectMode: true,
        });
        // Setup private data.
        this.config = config;
        this.droppedAltered = false;

        // Configure a new connection object.
        this.connection = new Connection({
            user: config.user,
            password: config.password || config.pass,
            server: config.url,
            port: config.port,
            database: config.database,
            driver: 'tedious',
            requestTimeout: 5 * 60 * 1000,
        });

        // Close connection after we push a null value.
        this.on('end', ::this.closeConnection);
    }
    
    /**
     * Get existing connection to MSSQL or create a new one.
     * @returns {Connection} A MSSQL connection.
     */
    async getConnection() {
        // If we've already connected, pool will exist.
        if (this.connection.pool) return this.connection;

        // Otherwise, wait for connection and return it.
        return await this.connection.connect();
    }

    /**
     * Close all active connections in pool.
     */
    closeConnection() {
        if (this.connection.pool) {
            this.connection.close();
        }
    }

    getValues(objectArray) {
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

        return joinedValues;
    }

    async executeQuery(chunk, incomingSchema) {
        // Get column names
        const columnNames = incomingSchema.map(field => field.name).join(',');

        // convert chunks to values
        const objectArray = chunk.toString().split('\n');

        this.log.debug('Getting column values.');
        const values = getValues(objectArray);
        // Create query with inserted values
        const query = `INSERT INTO ${this.config.table} (${columnNames}) VALUES ${values}`;

        this.connection.query(query, (err, results) => {
            if (err) { this.log.debug('Did NOT successfully insert into MySQL database: ' + err) };
            if (results) { this.log.debug('Sucessfully ran INSERT to MySQL database.') };
        })
    }

    databaseSetup(chunk) {
        const incomingSchema = await inferSchema(chunk, this.config.schemaHint, this.config.json);

        // Produce a schema preparer.
        if (!this.droppedAltered) {
            // Create sqlserver connection object from configuration.
            const connection = Object.assign({ ssl: true }, this.config.connection);
            // Get a knex database connection for setting up schema/table.
            const knexdb = knex({ client: 'sqlserver', connection });

            const schemaPreparer = this.config.drop
                ? new DropSchemaStrategy(knexdb)
                : new AlterSchemaStrategy(knexdb);

            // Prepare the schema for the INSERT.
            await schemaPreparer.prepare(this.config, incomingSchema, this.connection);
            this.droppedAltered = true;
            await knexdb.destroy();
        }

        await this.executeQuery(chunk, incomingSchema);
    }

    _write(chunk, encoding, next) {
        this.databaseSetup(chunk);
    }

    _writev(chunks, next) {
    }
}
