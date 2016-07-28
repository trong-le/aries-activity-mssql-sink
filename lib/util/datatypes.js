/**
 * These provide a mapping between the datatype that is infered,
 * and other data associated with it.
 * fn: Function called on knex table to create the column.
 * args: Additional args to apply to column creation function.
 * dt: Datatype, as redshift calls it, when queried.
 */
export default {
    'string': {
        fn: 'string',
        args: [],
        dt: 'character varying',
        priority: 0,
    },
    'number': {
        fn: 'decimal',
        args: [20, 5],
        dt: 'numeric',
        priority: 1,
    },
    'integer': {
        fn: 'integer',
        args: [],
        dt: 'integer',
        priority: 2,
    },
    'boolean': {
        fn: 'boolean',
        args: [],
        dt: 'boolean',
        priority: 3,
    },
    'datetime': {
        fn: 'timestamp',
        args: [true],
        dt: 'timestamp without time zone',
        priority: 4,
    },
};
