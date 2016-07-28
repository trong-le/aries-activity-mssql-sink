import SchemaStrategy from './SchemaStrategy';
import datatypes from './util/datatypes.js';

export default class AlterSchemaStrategy extends SchemaStrategy {
    /**
     * Query for current target table schema.  Then compare it with
     * the incoming schema.  Then create new columns for new fields
     * and/or alter columns to accomodate new information
     * from the incoming schema.
     */
    async prepare(config, incomingSchema) {
        // Create table if it does not exist.
        await this.db.schema.withSchema(config.schema)
            .createTableIfNotExists(config.table, this.getSchemaBuilder(incomingSchema));

        // Query for current table schema.
        const currentSchema = await this.db
            .select('table_name', 'column_name', 'data_type')
            .from('information_schema.columns')
            .where({
                table_schema: config.schema,
                table_name: config.table
            });
        this.log.info(currentSchema);

        // Create missing columns if any.
        await this.createMissingColumns(config, currentSchema, incomingSchema);

        // Alter columns if their types changed.
        await this.alterChangedColumns(config, currentSchema, incomingSchema);
    }

    /**
     * Check if we have missing columns, and create them if we do.
     */
    async createMissingColumns(config, currentSchema, incomingSchema) {
        // Check for any new columns.
        const newFields = this.checkForNewFields(currentSchema, incomingSchema);

        // If there are none, return.
        if (newFields.length === 0) return;
        this.log.info('New columns found.', newFields);

        // Redshift only permits one alter column at a time,
        // so loop through sequentially.
        for (const field of newFields) {
            await this.db.schema.withSchema(config.schema)
                .table(config.table, this.getSchemaBuilder([field]));
        }
        this.log.info('Created new columns.');
    }

    async alterChangedColumns(config, currentSchema, incomingSchema) {
        const self = this;

        // Check for any column type changes, and modify the field names to have _new appended.
        const changedFields = this.checkForFieldChanges(currentSchema, incomingSchema);

        // If there are none, return.
        if (changedFields.length === 0) return;
        this.log.info('Changed columns found.', changedFields);

        // Run a transaction that does the alters the columns (the hard way).
        await this.db.transaction(async function(trx) {
            // Create temporary columns, one at a time (redshift thing).
            for (const field of changedFields) {
                const tempField = Object.assign({}, field, { name: `${field.name}_temp` });
                await trx.schema.withSchema(config.schema)
                    .table(config.table, self.getSchemaBuilder([tempField]));
            }
            self.log.info('Created temp columns.');

            // Copy all values from original column to temporary version.
            const sets = changedFields.map((field, i) => `${field.name}_temp = ${field.name}`);
            await trx.raw(`UPDATE ${config.schema}.${config.table} SET ${sets.join(',')}`);
            self.log.info('Copied original values to temp columns.');

            // Drop original columns.
            await trx.schema.withSchema(config.schema).table(config.table, table => {
                changedFields.forEach(field => table.dropColumn(field.name));
            });
            self.log.info('Dropped original columns.');

            // Rename all temporary columns back to original names.
            await trx.schema.withSchema(config.schema).table(config.table, table => {
                changedFields.forEach(field => table.renameColumn(`${field.name}_temp`, field.name));
            });
            self.log.info('Renamed temp columns back to original names.');
        });
    }

    /**
     * Check if there are any new columns in the incoming schema.
     */
    checkForNewFields(currentFields, incomingFields) {
        const newColumns = incomingFields.map(field => {
            // Find the current schema row for this field name.
            const row = currentFields.find(row => {
                return row.column_name.toLowerCase() === field.name.toLowerCase();
            });

            // If field not in current schema, return it.
            if (!row) return field;
        });

        // Remove undefineds.
        return newColumns.filter(Boolean);
    }

    /**
     * Check if there are any columns that have changed.
     */
    checkForFieldChanges(currentFields, incomingFields) {
        const changes = incomingFields.map(field => {
            // Find the current schema row for this field name.
            const row = currentFields.find(row => {
                return row.column_name.toLowerCase() === field.name.toLowerCase();
            });

            // Return early if it does not exist yet.
            if (!row) return;

            // Convert redshift notation type to same notation we infer.
            const types = Object.keys(datatypes);
            const current = types.find(type => row.data_type === datatypes[type].dt);

            // Check if we allow the change.
            const shouldChange = this.shouldChangeType(current, field.type);

            // Return field if we allow this change.
            if (shouldChange) return field;
        });

        // Remove undefineds.
        return changes.filter(Boolean);
    }

    /**
     * Sometimes data times may change over time, as we recieve more data.
     * Logically, only some changes make sense.
     */
    shouldChangeType(current, incoming) {
        // Return if we can upgrade the type to the incoming one.
        if (current === incoming) return false;
        if (current === 'integer' && incoming === 'number') return true;
        if (current === 'integer' && incoming === 'string') return true;
        if (current === 'number' && incoming === 'string') return true;
        if (current === 'boolean' && incoming === 'integer') return true;
        if (current === 'boolean' && incoming === 'number') return true;
        if (current === 'boolean' && incoming === 'string') return true;
        return false;
    }
};
