import { Injectable } from '@nestjs/common';
import { createAdapter } from '@socket.io/postgres-adapter';
import { Pool } from 'pg';
import { Server as SocketIO } from 'socket.io';
import { ExitCode } from 'src/enum.js';
import { SOCKET_IO_ATTACHMENTS_SQL } from 'src/middleware/websocket.adapter.js';
import { ConfigRepository } from 'src/repositories/config.repository.js';
import { AppRestartEvent } from 'src/repositories/event.repository.js';
import { getPgPoolConfig } from 'src/utils/database.js';

@Injectable()
export class AppRepository {
  private closeFn?: () => Promise<void>;

  exitApp() {
    /* eslint-disable unicorn/no-process-exit */
    void this.closeFn?.().finally(() => process.exit(ExitCode.AppRestart));

    // in exceptional circumstance, the application may hang
    setTimeout(() => process.exit(ExitCode.AppRestart), 2000);
    /* eslint-enable unicorn/no-process-exit */
  }

  setCloseFn(fn: () => Promise<void>) {
    this.closeFn = fn;
  }

  async sendOneShotAppRestart(state: AppRestartEvent): Promise<void> {
    const { database } = new ConfigRepository().getEnv();
    const pool = new Pool({ ...getPgPoolConfig(database.config), max: 2 });
    const server = new SocketIO();

    try {
      await pool.query(SOCKET_IO_ATTACHMENTS_SQL);
      const adapterReady = new Promise<void>((resolve, reject) => {
        pool.once('acquire', (client) => {
          setImmediate(
            () =>
              void client
                .query('SELECT 1')
                .then(() => resolve())
                .catch(reject),
          );
        });
      });
      server.adapter(createAdapter(pool));
      await adapterReady;

      server.emit('AppRestartV1', state);
      const responses = await server.serverSideEmitWithAck('AppRestart', state);
      if (responses.some((response) => response !== 'ok')) {
        throw new Error("One or more node(s) returned a non-'ok' response to our restart request!");
      }
    } finally {
      await server.close();
      await pool.end();
    }
  }
}
