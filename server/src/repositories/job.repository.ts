import { Injectable } from '@nestjs/common';
import { ModuleRef, Reflector } from '@nestjs/core';
import {
  assertSchemaCompatibility,
  createPostgresBackend,
  JobsOptions,
  JobType,
  PostgresConnectionOptions,
  PostgresQueueBackend,
  Queue,
  runMigrations,
  Worker,
} from 'bullmq';
import { setTimeout } from 'node:timers/promises';
import { Pool } from 'pg';
import { JobConfig } from 'src/decorators.js';
import { QueueJobResponseDto, QueueJobSearchDto } from 'src/dtos/queue.dto.js';
import { ImmichWorker, JobName, JobStatus, MetadataKey, QueueCleanType, QueueJobStatus, QueueName } from 'src/enum.js';
import { ConfigRepository } from 'src/repositories/config.repository.js';
import { EventRepository } from 'src/repositories/event.repository.js';
import { LoggingRepository } from 'src/repositories/logging.repository.js';
import type { JobCounts, JobItem, JobOf } from 'src/types.js';
import { getPgPoolConfig } from 'src/utils/database.js';
import { ImmichStartupError, getKeyByValue, getMethodNames } from 'src/utils/misc.js';

type JobMapItem = {
  jobName: JobName;
  queueName: QueueName;
  handler: (job: JobOf<any>) => Promise<JobStatus>;
  label: string;
};

const WORKER_WATCH_INTERVAL_MS = 30_000;

@Injectable()
export class JobRepository {
  private workers: Partial<
    Record<QueueName, Worker<any, any, string, PostgresQueueBackend, any, PostgresConnectionOptions>>
  > = {};
  private queues: Partial<
    Record<QueueName, Queue<any, any, string, any, any, string, PostgresQueueBackend, PostgresConnectionOptions>>
  > = {};
  private handlers: Partial<Record<JobName, JobMapItem>> = {};
  private workerWatcher?: ReturnType<typeof setInterval>;
  private microservicesPresent = true;
  private pool?: Pool;

  constructor(
    private moduleRef: ModuleRef,
    private configRepository: ConfigRepository,
    private eventRepository: EventRepository,
    private logger: LoggingRepository,
  ) {
    this.logger.setContext(JobRepository.name);
  }

  async setup(services: (new (...args: any[]) => unknown)[]) {
    const reflector = this.moduleRef.get(Reflector, { strict: false });

    for (const Service of services) {
      const instance = this.moduleRef.get<any>(Service);
      for (const methodName of getMethodNames(instance)) {
        const handler = instance[methodName];
        const config = reflector.get<JobConfig>(MetadataKey.JobConfig, handler);
        if (!config) {
          continue;
        }

        const { name: jobName, queue: queueName } = config;
        const label = `${Service.name}.${handler.name}`;

        if (Object.hasOwn(this.handlers, jobName)) {
          const jobKey = getKeyByValue(JobName, jobName);
          const errorMessage = `Failed to add job handler for ${label}`;
          this.logger.error(
            `${errorMessage}. JobName.${jobKey} is already handled by ${this.handlers[jobName]!.label}.`,
          );
          throw new ImmichStartupError(errorMessage);
        }

        this.handlers[jobName] = {
          label,
          jobName,
          queueName,
          handler: handler.bind(instance),
        };

        this.logger.verbose(`Added job handler: ${jobName} => ${label}`);
      }
    }

    for (const [jobKey, jobName] of Object.entries(JobName)) {
      const item = this.handlers[jobName];
      if (!item) {
        const errorMessage = `Failed to find job handler for Job.${jobKey} ("${jobName}")`;
        this.logger.error(
          `${errorMessage}. Make sure to add the @OnJob({ name: JobName.${jobKey}, queue: QueueName.XYZ }) decorator for the new job.`,
        );
        throw new ImmichStartupError(errorMessage);
      }
    }

    const { database, bull } = this.configRepository.getEnv();
    const queueNames = Object.values(QueueName);
    this.pool = new Pool({
      ...getPgPoolConfig(database.config),
      options: '-c search_path=bullmq,public',
      keepAlive: true,
      max: this.configRepository.getWorker() === ImmichWorker.Microservices ? queueNames.length + 8 : 8,
    });
    this.pool.setMaxListeners(queueNames.length * 2 + 1);

    try {
      const client = await this.pool.connect();
      try {
        if (database.skipMigrations) {
          await assertSchemaCompatibility(client);
        } else {
          await runMigrations(client);
        }
      } finally {
        client.release();
      }

      for (const queueName of queueNames) {
        this.queues[queueName] = new Queue(queueName, { connection: this.pool, ...bull.config }, createPostgresBackend);
      }
    } catch (error) {
      await this.teardown();
      throw error;
    }
  }

  startWorkers() {
    const { bull } = this.configRepository.getEnv();
    for (const queueName of Object.values(QueueName)) {
      this.logger.debug(`Starting worker for queue: ${queueName}`);
      this.workers[queueName] = new Worker(
        queueName,
        (job) => this.eventRepository.emit('JobRun', queueName, job as JobItem),
        { connection: this.pool!, ...bull.config, concurrency: 1, name: ImmichWorker.Microservices },
        createPostgresBackend,
      );
    }
  }

  watchWorkers() {
    this.workerWatcher ??= setInterval(() => void this.checkWorkers(), WORKER_WATCH_INTERVAL_MS);
  }

  async teardown() {
    if (this.workerWatcher) {
      clearInterval(this.workerWatcher);
      this.workerWatcher = undefined;
    }

    await Promise.all(Object.values(this.workers).map((worker) => worker.close()));
    this.workers = {};
    await Promise.all(Object.values(this.queues).map((queue) => queue.close()));
    this.queues = {};
    await this.pool?.end();
    this.pool = undefined;
  }

  private async checkWorkers() {
    let isPresent: boolean;
    try {
      const workers = await this.getQueue(QueueName.BackgroundTask).getWorkers();
      const suffix = `:w:${ImmichWorker.Microservices}`;
      isPresent = workers.some(
        (worker) => worker.name === ImmichWorker.Microservices || worker.rawname?.endsWith(suffix),
      );
    } catch {
      return;
    }

    if (this.microservicesPresent !== isPresent) {
      if (isPresent) {
        this.logger.log('Microservices worker connected.');
      } else {
        this.logger.warn(
          'No microservices worker is connected. Background jobs will not be processed until one is running.',
        );
      }
    }
    this.microservicesPresent = isPresent;
  }

  async run({ name, data }: JobItem) {
    const item = this.handlers[name as JobName];
    if (!item) {
      this.logger.warn(`Skipping unknown job: "${name}"`);
      return JobStatus.Skipped;
    }

    return item.handler(data);
  }

  setConcurrency(queueName: QueueName, concurrency: number) {
    const worker = this.workers[queueName];
    if (!worker) {
      this.logger.warn(`Unable to set queue concurrency, worker not found: '${queueName}'`);
      return;
    }

    worker.concurrency = concurrency;
  }

  async isActive(name: QueueName): Promise<boolean> {
    const queue = this.getQueue(name);
    const count = await queue.getActiveCount();
    return count > 0;
  }

  async isPaused(name: QueueName): Promise<boolean> {
    return this.getQueue(name).isPaused();
  }

  pause(name: QueueName) {
    return this.getQueue(name).pause();
  }

  resume(name: QueueName) {
    return this.getQueue(name).resume();
  }

  empty(name: QueueName) {
    return this.getQueue(name).drain();
  }

  clear(name: QueueName, type: QueueCleanType) {
    return this.getQueue(name).clean(0, 1000, type);
  }

  async getJobCounts(name: QueueName): Promise<JobCounts> {
    return this.getQueue(name).getJobCounts(
      'active',
      'completed',
      'failed',
      'delayed',
      'waiting',
    ) as unknown as Promise<JobCounts>;
  }

  private getQueueName(name: JobName) {
    return (this.handlers[name] as JobMapItem).queueName;
  }

  async queueAll(items: JobItem[]): Promise<void> {
    if (items.length === 0) {
      return;
    }

    const promises = [];
    const itemsByQueue = {} as Record<string, (JobItem & { data: any; options: JobsOptions | undefined })[]>;
    for (const item of items) {
      const queueName = this.getQueueName(item.name);
      const job = {
        name: item.name,
        data: item.data || {},
        options: this.getJobOptions(item) || undefined,
      } as JobItem & { data: any; options: JobsOptions | undefined };

      if (job.options?.jobId || job.options?.deduplication) {
        promises.push(this.getQueue(queueName).add(item.name, item.data, job.options));
      } else {
        itemsByQueue[queueName] ||= [];
        itemsByQueue[queueName].push(job);
      }
    }

    for (const [queueName, jobs] of Object.entries(itemsByQueue)) {
      const queue = this.getQueue(queueName as QueueName);
      promises.push(queue.addBulk(jobs));
    }

    await Promise.all(promises);
  }

  async queue(item: JobItem): Promise<void> {
    return this.queueAll([item]);
  }

  async waitForQueueCompletion(...queues: QueueName[]): Promise<void> {
    const getPending = async () => {
      const results = await Promise.all(queues.map(async (name) => ({ pending: await this.isActive(name), name })));
      return results.filter(({ pending }) => pending).map(({ name }) => name);
    };

    let pending = await getPending();

    while (pending.length > 0) {
      this.logger.verbose(`Waiting for ${pending[0]} queue to stop...`);
      await setTimeout(1000);
      pending = await getPending();
    }
  }

  async searchJobs(name: QueueName, dto: QueueJobSearchDto): Promise<QueueJobResponseDto[]> {
    const queue = this.getQueue(name);
    const isPaused = await queue.isPaused();
    const statuses = (dto.status ?? Object.values(QueueJobStatus))
      .filter((status) =>
        status === QueueJobStatus.Paused ? isPaused : !(isPaused && status === QueueJobStatus.Waiting),
      )
      .map((status) => (status === QueueJobStatus.Paused ? QueueJobStatus.Waiting : status)) as JobType[];
    if (statuses.length === 0) {
      return [];
    }
    const jobs = await queue.getJobs(statuses, 0, 1000);
    return jobs.map((job) => {
      const { id, name, timestamp, data } = job;
      return { id, name: name as JobName, timestamp, data };
    });
  }

  private getJobOptions(item: JobItem): JobsOptions | null {
    switch (item.name) {
      case JobName.NotifyAlbumUpdate: {
        return {
          jobId: `${item.data.id}/${item.data.recipientId}`,
          delay: item.data?.delay,
        };
      }
      case JobName.StorageTemplateMigrationSingle: {
        return { jobId: item.data.id };
      }
      case JobName.PersonGenerateThumbnail: {
        return { priority: 1 };
      }
      case JobName.FacialRecognitionQueueAll: {
        return { deduplication: { id: JobName.FacialRecognitionQueueAll } };
      }
      case JobName.VersionCheck: {
        return { deduplication: { id: JobName.VersionCheck } };
      }
      case JobName.DatabaseBackup: {
        return { deduplication: { id: JobName.DatabaseBackup } };
      }
      default: {
        return null;
      }
    }
  }

  private getQueue(
    queue: QueueName,
  ): Queue<any, any, string, any, any, string, PostgresQueueBackend, PostgresConnectionOptions> {
    const instance = this.queues[queue];
    if (!instance) {
      throw new Error(`Queue not initialized: ${queue}`);
    }
    return instance;
  }

  /** @deprecated */
  // todo: remove this when asset notifications no longer need it.
  public async removeJob(name: JobName, jobID: string): Promise<void> {
    const existingJob = await this.getQueue(this.getQueueName(name)).getJob(jobID);
    if (existingJob) {
      await existingJob.remove();
    }
  }
}
