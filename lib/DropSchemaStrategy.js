import SchemaStrategy from './SchemaStrategy';

export default class DropSchemaStrategy extends SchemaStrategy {
    /**
     * Drop the target table, then recreate it using the incoming schema.
     */
    async prepare(config, incomingSchema) {
        this.log.debug('Dropping schema.');

        // Have to change this so that pg-promise works
        await this.db.schema.withSchema(config.schema)
            .dropTableIfExists(config.table);

        // Create fresh table with incoming schema.
        await this.db.schema.withSchema(config.schema)
            .createTable(config.table, this.getSchemaBuilder(incomingSchema));
    }
};
