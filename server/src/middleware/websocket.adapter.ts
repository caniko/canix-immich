import { INestApplicationContext } from '@nestjs/common';
import { IoAdapter } from '@nestjs/platform-socket.io';
import { createAdapter } from '@socket.io/postgres-adapter';
import { Pool } from 'pg';
import { ServerOptions } from 'socket.io';
import { ConfigRepository } from 'src/repositories/config.repository.js';
import { getPgPoolConfig } from 'src/utils/database.js';

export const SOCKET_IO_ATTACHMENTS_SQL = `
  CREATE TABLE IF NOT EXISTS socket_io_attachments (
    id          bigserial UNIQUE,
    created_at  timestamptz DEFAULT NOW(),
    payload     bytea
  );
`;

export const createWebSocketAdapter = async (app: INestApplicationContext): Promise<WebSocketAdapter> => {
  const { database } = app.get(ConfigRepository).getEnv();
  // Each Socket.IO server's adapter pins one client for LISTEN; leave pg's default headroom for queries.
  const pool = new Pool(getPgPoolConfig(database.config));
  try {
    await pool.query(SOCKET_IO_ATTACHMENTS_SQL);
    return new WebSocketAdapter(app, pool);
  } catch (error) {
    await pool.end();
    throw error;
  }
};

export class WebSocketAdapter extends IoAdapter {
  constructor(
    app: INestApplicationContext,
    private pool: Pool,
  ) {
    super(app);
  }

  createIOServer(port: number, options?: ServerOptions): any {
    const server = super.createIOServer(port, options);
    const isImmichSocket = !options?.path || options.path === '/api/socket.io';
    // Only override the default channel prefix for non-Immich socket servers, so the
    // adapter's own defaults stay intact (Object.assign would let an explicit
    // `undefined` clobber them).
    const adapterOpts = !isImmichSocket && options?.path ? { channelPrefix: `socket.io${options.path}` } : {};
    server.adapter(createAdapter(this.pool, adapterOpts));
    return server;
  }

  // Nest calls close() once per server, then dispose() once: end the shared pool here.
  async dispose() {
    await super.dispose();
    await this.pool.end();
  }
}
