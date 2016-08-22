import test from 'blue-tape';
import SQLServerSink from '..';
import config from './test.config';
import fs from 'fs';
import knex from 'knex';

test('proper configuration', t => {
    t.equal(SQLServerSink.props.name, require('../package.json').name);
    t.equal(SQLServerSink.props.version, require('../package.json').version);
    t.end();
});

test('test incoming schema csv', async (t) => {
    const source = new SQLServerSink();
	const fileStream = fs.createReadStream('lib/typeform');
	await source.onTaskCopy(fileStream, config);
});

test('test insert sqlserver', async (t) => {
    const source = new SQLServerSink();
  	const fileStream = fs.createReadStream('lib/testFile');
  	const connection = Object.assign({ ssl: false }, config.connection);
 	// Get a knex database connection for setting up schema/table.
 	const knexdb = knex({ client: 'sqlserver', connection });
 	await source.insertsqlserver(fileStream, config, knexdb);
 	await knexdb.destroy();
});
