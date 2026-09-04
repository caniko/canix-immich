import { createPostgres } from '@immich/sql-tools';
import { createAdapter } from '@socket.io/postgres-adapter';
import { createServer } from 'node:http';
import { setTimeout } from 'node:timers/promises';
import { Pool } from 'pg';
import { Server } from 'socket.io';
import { SOCKET_IO_ATTACHMENTS_SQL } from 'src/middleware/websocket.adapter.js';

const templateName = 'mich';
const withDatabase = (url: string, name: string) => url.replace(`/${templateName}`, () => `/${name}`);

const newPool = async () => {
  const testUrl = process.env.IMMICH_TEST_POSTGRES_URL!;
  const sql = createPostgres({
    maxConnections: 1,
    connection: { connectionType: 'url', url: withDatabase(testUrl, 'postgres') },
  });
  const dbName = `immich_sio_${Math.random().toString(36).slice(2, 7)}`;
  await sql.unsafe(`CREATE DATABASE ${dbName} WITH TEMPLATE ${templateName} OWNER postgres;`);
  await sql.end();
  const pool = new Pool({ connectionString: withDatabase(testUrl, dbName) });
  await pool.query(SOCKET_IO_ATTACHMENTS_SQL);
  return pool;
};

describe('socket.io postgres adapter', () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = await newPool();
  });

  afterAll(async () => {
    await pool.end();
  });

  it('broadcasts server-side acks including large payloads', async () => {
    const a = new Server(createServer(), { adapter: createAdapter(pool) });
    const b = new Server(createServer(), { adapter: createAdapter(pool) });

    try {
      await setTimeout(200);

      b.on('hello', (cb: (value: string) => void) => cb('ok'));
      const acks = await a.serverSideEmitWithAck('hello');
      expect(acks).toContain('ok');

      const payload = 'x'.repeat(9000);
      b.on('big', (data: string, cb: (value: number) => void) => cb(data.length));
      const sizes = await a.serverSideEmitWithAck('big', payload);
      expect(sizes).toContain(9000);
    } finally {
      await Promise.all([a.close(), b.close()]);
    }
  });
});
