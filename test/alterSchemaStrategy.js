import test from 'blue-tape';
import AlterSchemaStrategy from '../lib/AlterSchemaStrategy';

test('detects no new columns', t => {
    const strat = new AlterSchemaStrategy({ db: {} });

    const current = [
        { table_name: 'test', column_name: 'col1', data_type: 'numeric' },
        { table_name: 'test', column_name: 'col2', data_type: 'integer' },
    ];

    const incoming = [
        { name: 'col1', type: 'number' },
        { name: 'col2', type: 'integer' },
    ];

    const newColumns = strat.checkForNewFields(current, incoming);
    t.equal(newColumns.length, 0);
    t.end();
});

test('detects new columns', t => {
    const strat = new AlterSchemaStrategy({ db: {} });

    const current = [
        { table_name: 'test', column_name: 'col1', data_type: 'numeric' },
        { table_name: 'test', column_name: 'col2', data_type: 'integer' },
    ];

    const incoming = [
        { name: 'col1', type: 'number' },
        { name: 'col2', type: 'integer' },
        { name: 'col3', type: 'string' },
        { name: 'col4', type: 'datetime' },
    ];

    const newColumns = strat.checkForNewFields(current, incoming);
    t.equal(newColumns.length, 2);
    t.end();
});

test('detects no schema changes', t => {
    const strat = new AlterSchemaStrategy({ db: {} });

    const current = [
        { table_name: 'test', column_name: 'col1', data_type: 'numeric' },
        { table_name: 'test', column_name: 'col2', data_type: 'integer' },
    ];

    const incoming = [
        { name: 'col1', type: 'number' },
        { name: 'col2', type: 'integer' },
    ];

    const changes = strat.checkForFieldChanges(current, incoming);
    t.equal(changes.length, 0);
    t.end();
});

test('detects schema changes', t => {
    const strat = new AlterSchemaStrategy({ db: {} });

    const current = [
        { table_name: 'test', column_name: 'col1', data_type: 'numeric' },
        { table_name: 'test', column_name: 'col2', data_type: 'integer' },
    ];

    const incoming = [
        { name: 'col1', type: 'number' },
        { name: 'col2', type: 'number' },
    ];

    const changes = strat.checkForFieldChanges(current, incoming);
    t.equal(changes.length, 1);
    t.end();
});

test('allows valid type change', t => {
    const strat = new AlterSchemaStrategy({ db: {} });
    const isValid = strat.shouldChangeType('integer', 'number');
    t.equal(isValid, true);
    t.end();
});

test('denies invalid type change', t => {
    const strat = new AlterSchemaStrategy({ db: {} });
    const isValid = strat.shouldChangeType('number', 'boolean');
    t.equal(isValid, false);
    t.end();
});
