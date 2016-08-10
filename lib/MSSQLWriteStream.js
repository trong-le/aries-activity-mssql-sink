import { logger } from 'aries-data';
import { Writable } from 'stream';
import { Connection, Request } from 'mssql';

@logger()
export default class MSSQLWriteStream extends Writable {

    constructor(config) {
        super({
            // objectMode: true,
            // highWaterMark: MSSQLReadStream.BATCH_SIZE,
        });
        // Setup private data.
        this.config = config;

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

    _write(chunk, encoding, next) {
    }

    _writev(chunks, next) {
    }
}
