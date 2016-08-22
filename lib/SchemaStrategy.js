import { logger } from 'aries-data';
import curry from 'lodash.curry';
import datatypes from './util/datatypes';

@logger()
export default class SchemaStrategy {
    constructor(db) {
        if (!db) throw new Error('Database connection required!');
        this.db = db;
    }

    // Curry joins everything into array
    getSchemaBuilder(incomingSchema) {
        return curry(this.buildSchema.bind(this))(incomingSchema);
    }

    /**
     * Take fields and create the tables on the table object.
     * TODO: Figure out how to dynamically figure out column sizes.
     * May need to fork and modify jts-infer.
     */
    buildSchema(fields, table) {
        fields.forEach(field => {
            // Get our type mapping.
            const mapping = datatypes[field.type];

            // Create argument list.
            const args = mapping.args.slice(0);
            args.unshift(field.name);

            // Create the column.
            table[mapping.fn].apply(table, args);
        });
    }
};
