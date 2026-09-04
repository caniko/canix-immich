import { createPostgres } from '@immich/sql-tools';
import { createPostgresBackend, JobType, Queue, runMigrations, Worker } from 'bullmq';
import { setTimeout } from 'node:timers/promises';
import { Pool } from 'pg';
import { ImmichWorker, QueueCleanType, QueueName } from 'src/enum.js';

const templateName = 'mich';
const withDatabase = (url: string, name: string) => url.replace(`/${templateName}`, () => `/${name}`);

const newPool = async () => {
  const testUrl = process.env.IMMICH_TEST_POSTGRES_URL!;
  const sql = createPostgres({
    maxConnections: 1,
    connection: { connectionType: 'url', url: withDatabase(testUrl, 'postgres') },
  });
  const dbName = `immich_bull_${Math.random().toString(36).slice(2, 7)}`;
  await sql.unsafe(`CREATE DATABASE ${dbName} WITH TEMPLATE ${templateName} OWNER postgres;`);
  await sql.end();
  const pool = new Pool({
    connectionString: withDatabase(testUrl, dbName),
    options: '-c search_path=bullmq,public',
  });
  const client = await pool.connect();
  try {
    await runMigrations(client);
  } finally {
    client.release();
  }
  return pool;
};

describe('BullMQ postgres backend', () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = await newPool();
  });

  afterAll(async () => {
    await pool.end();
  });

  it('adds and processes a job, honors jobId, discovers workers, pause/resume, and clean', async () => {
    const queue = new Queue(
      QueueName.BackgroundTask,
      { connection: pool, defaultJobOptions: { attempts: 1, removeOnComplete: false, removeOnFail: false } },
      createPostgresBackend,
    );
    const processed: string[] = [];
    const worker = new Worker(
      QueueName.BackgroundTask,
      (job) => {
        processed.push(String(job.id));
        if (job.data.fail) {
          return Promise.reject(new Error('fail'));
        }
        return Promise.resolve();
      },
      { connection: pool, concurrency: 1, name: ImmichWorker.Microservices },
      createPostgresBackend,
    );

    try {
      await worker.waitUntilReady();

      await queue.add('ok', { n: 1 }, { jobId: 'same' });
      await queue.add('ok', { n: 2 }, { jobId: 'same' });

      const deadline = Date.now() + 10_000;
      while (processed.length === 0 && Date.now() < deadline) {
        await setTimeout(50);
      }
      expect(processed).toEqual(['same']);

      const workers = await queue.getWorkers();
      const suffix = `:w:${ImmichWorker.Microservices}`;
      expect(workers.some((item) => item.name === ImmichWorker.Microservices || item.rawname?.endsWith(suffix))).toBe(
        true,
      );

      await queue.pause();
      expect(await queue.isPaused()).toBe(true);
      await queue.add('paused', {}, { jobId: 'paused' });
      expect(await queue.getJobCounts('waiting', 'paused' as JobType)).toEqual({ waiting: 0, paused: 1 });
      await queue.resume();
      expect(await queue.isPaused()).toBe(false);

      await queue.add('boom', { fail: true }, { jobId: 'fail-me' });
      const failDeadline = Date.now() + 10_000;
      while ((await queue.getFailedCount()) < 1 && Date.now() < failDeadline) {
        await setTimeout(50);
      }
      const cleaned = await queue.clean(0, 1000, QueueCleanType.Failed);
      expect(cleaned.length).toBeGreaterThan(0);
    } finally {
      await worker.close();
      await queue.close();
    }
  });
});
