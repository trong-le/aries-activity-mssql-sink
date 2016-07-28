import infer from 'jts-infer';
import datatypes from './datatypes.js';
import _ from 'highland';

/**
 * Infer a schema from a read stream CSV.
 *
 * schema.fields -> [ { name, type }, { name, type } ]
 * scores -> [ { string: x }, { integer: x }, { date: x }, { datetime: x }, { boolean: x } ]
 * @returns schema
 */
export default async function(stream, schemaHint = [], json = false) {
    return await new Promise((resolve, reject) => {
        // TODO: use a package
        if (json) {
            const wrapped = _(stream).split().map(JSON.parse).errors(err => {});

            // Incoming schema as we stream through.
            const schema = [];

            wrapped.on('data', obj => {
                // Loop through all keys, add if not in schema.
                const newFields = Object.keys(obj).map(key => {
                    // Check if we already found this column and return early if we do.
                    const match = schema.find(field => field.name === key);
                    if (match) return;

                    // Check if we have a hint, and return early if we do.
                    const hintedField = schemaHint.find(col => col.name === key);
                    if (hintedField) return hintedField;

                    // Return early if it's not in datatypes (i.e. null/undefined)
                    if (!datatypes[typeof obj[key]]) return;

                    // Infer type of this field.
                    const inferredType = datatypes[typeof obj[key]].fn;

                    // If its a decimal, determine integer or number type.
                    const type = inferredType === 'decimal'
                        ? ((obj[key].toString().indexOf('.') === -1) ? 'integer' : 'number')
                        : inferredType;

                    // Return name, type object.
                    return { name: key, type };
                });

                // Push new fields, remove any undefined (already exists).
                schema.push(...newFields.filter(Boolean));
            });

            stream.on('end', () => {
                resolve(schema);
            });

            stream.on('error', (err) => {
                reject(err);
            });
        } else {
            infer(stream, (error, schema, scores) => {
                if (error) return reject(error);
                // resolve(schema.fields);

                const fields = schema.fields.map((field) => {
                    // This fields scores.
                    // const fieldScores = scores[i];

                    // Filter out goose eggs.
                    // const possibleTypes = Object.keys(fieldScores).filter(key => fieldScores[key] > 0);

                    // Get the type we should use by using the minimal priority.
                    // const type = possibleTypes.reduce((prev, curr, i) => {
                    //     if (!prev) return curr;
                    //     return datatypes[curr] < datatypes[prev] ? curr : prev;
                    // });

                    // Mash up a new object with the type we defined.
                    // return Object.assign({}, field, { type });

                    const hintedField = schemaHint.find(col => col.name === field.name);

                    return hintedField || field;
                });

                return resolve(fields);
            });
        }
    });
};