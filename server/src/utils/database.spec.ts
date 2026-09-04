import { getPgPoolConfig } from 'src/utils/database.js';

describe('getPgPoolConfig', () => {
  it('maps a connection URL', () => {
    expect(getPgPoolConfig({ connectionType: 'url', url: 'postgres://u:p@db:5432/immich' })).toEqual({
      connectionString: 'postgres://u:p@db:5432/immich',
    });
  });

  it('maps connection parts including a unix socket host', () => {
    expect(
      getPgPoolConfig({
        connectionType: 'parts',
        host: '/var/run/postgresql',
        port: 5432,
        username: 'postgres',
        password: 'secret',
        database: 'immich',
      }),
    ).toEqual({
      host: '/var/run/postgresql',
      port: 5432,
      user: 'postgres',
      password: 'secret',
      database: 'immich',
      ssl: false,
    });
  });

  it('maps ssl modes', () => {
    expect(
      getPgPoolConfig({
        connectionType: 'parts',
        host: 'db',
        port: 5432,
        username: 'postgres',
        password: 'postgres',
        database: 'immich',
        ssl: 'allow' as never,
      }).ssl,
    ).toBe(false);

    expect(
      getPgPoolConfig({
        connectionType: 'parts',
        host: 'db',
        port: 5432,
        username: 'postgres',
        password: 'postgres',
        database: 'immich',
        ssl: 'verify-full' as never,
      }).ssl,
    ).toEqual({ rejectUnauthorized: true });

    expect(
      getPgPoolConfig({
        connectionType: 'parts',
        host: 'db',
        port: 5432,
        username: 'postgres',
        password: 'postgres',
        database: 'immich',
        ssl: 'require' as never,
      }).ssl,
    ).toEqual({ rejectUnauthorized: false });
  });
});
