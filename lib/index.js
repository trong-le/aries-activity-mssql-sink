import { Activity, singleS3StreamInput } from 'aries-data';
import pkgJson from '../package.json';
import streamToMSSql from './MSSQLWriteStream';

export default class SQLServerSink extends Activity {
    static props = {
        name: pkgJson.name,
        version: pkgJson.version,
    };

    @singleS3StreamInput()
    async onTask(activityTask, config) {
        await this.writeFromStream(activityTask.input.file, config);
    }

    // pipe all incoming data into MSSQLWriteStream
    writeFromStream(stream, options) {
        const writeStream = new streamToMSSql(optins);
        stream.pipe(JSON.parse()).pipe(writeStream);
    }
};