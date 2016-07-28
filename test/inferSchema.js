import test from 'blue-tape';
import fs from 'fs';
import inferSchema from '../lib/util/inferSchema';

test('infers correct schema', async (t) => {
    const stream = fs.createReadStream('test/fixtures/incomingSchema.csv');
    const schema = await inferSchema(stream);
    t.deepEqual(schema[0], { name: 'item', type: 'string' });
    t.deepEqual(schema[1], { name: 'family_group', type: 'string' });
    t.deepEqual(schema[2], { name: 'major_group', type: 'string' });
    t.deepEqual(schema[3], { name: 'gross_sales', type: 'number' });
    t.deepEqual(schema[4], { name: 'item_discounts', type: 'number' });
    t.deepEqual(schema[5], { name: 'sales_less_item_disc', type: 'number' });
    t.deepEqual(schema[6], { name: 'sales_percent', type: 'number' });
    t.deepEqual(schema[7], { name: 'qty_sold', type: 'integer' });
    t.deepEqual(schema[8], { name: 'qty_sold_percent', type: 'number' });
    t.deepEqual(schema[9], { name: 'average_price', type: 'number' });
    t.deepEqual(schema[10], { name: 'date', type: 'string' });
});
