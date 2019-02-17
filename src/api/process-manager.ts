import { startBuildProcess } from './process';
import { Observable, Subject, BehaviorSubject, Subscription, from, timer } from 'rxjs';
import { filter, mergeMap, share, map } from 'rxjs/operators';
import {
  insertBuild,
  updateBuild,
  getBuild,
  getBuildStatus,
  getLastRunId,
  getDepracatedBuilds,
  getLastBuild
} from './db/build';
import { insertBuildRun, updateBuildRun } from './db/build-run';
import * as dbJob from './db/job';
import * as dbJobRuns from './db/job-run';
import { getRepositoryOnly, getRepositoryByBuildId } from './db/repository';
import { getRemoteParsedConfig, JobsAndEnv, CommandType } from './config';
import { killContainer } from './docker';
import { logger, LogMessageType } from './logger';
import { getHttpJsonResponse, getBitBucketAccessToken } from './utils';
import { getConfig } from './setup';
import { sendFailureStatus, sendPendingStatus, sendSuccessStatus } from './commit-status';
import { decrypt } from './security';
import * as envVars from './env-variables';

export interface BuildMessage {
  type: string;
  jobMessage: JobMessage;
}

export interface JobMessage {
  build_id?: number;
  job_id?: number;
  image_name?: string;
  type: string;
  data: string | number;
}

export interface JobProcess {
  build_id?: number;
  job_id?: number;
  status?: 'queued' | 'running' | 'cancelled' | 'errored' | 'success';
  image_name?: string;
  log?: string;
  requestData: any;
  commands?: { command: string, type: CommandType }[];
  cache?: string[];
  repo_name?: string;
  branch?: string;
  env?: string[];
  job?: Observable<any>;
  exposed_ports?: string;
  debug?: boolean;
}

export interface JobProcessEvent {
  build_id?: number;
  job_id?: number;
  repository_id?: number;
  type?: string;
  data?: string;
  additionalData?: any;
}

let config: any = getConfig();
export let jobProcesses: Subject<JobProcess> = new Subject();
export let jobEvents: BehaviorSubject<JobProcessEvent> = new BehaviorSubject({});
export let terminalEvents: Subject<JobProcessEvent> = new Subject();
export let buildSub: { [id: number]: Subscription } = {};
export let processes: JobProcess[] = [];

jobEvents
  .pipe(
    filter(event => !!event.build_id && !!event.job_id),
    share()
  )
  .subscribe(event => {
    let msg: LogMessageType = {
      message: `[build]: build: ${event.build_id} job: ${event.job_id} => ${event.data}`,
      type: 'info',
      notify: false
    };

    logger.next(msg);
  });

// main scheduler
let concurrency = config.concurrency || 10;
jobProcesses
  .pipe(
    mergeMap(process => execJob(process), concurrency)
  )
  .subscribe();

function execJob(proc: JobProcess): Observable<any> {
  let index = processes.findIndex(process => process.job_id === proc.job_id);
  if (index !== -1) {
    processes[index] = proc;
  } else {
    processes.push(proc);
  }

  let buildProcesses = processes.filter(p => p.build_id === proc.build_id);
  let testProcesses = buildProcesses.filter(process => {
    return process.env.findIndex(e => e === 'DEPLOY') === -1;
  });
  let queuedOrRunning = testProcesses.filter(p => {
    return p.status === 'queued' || p.status === 'running';
  });
  let succeded = testProcesses.filter(p => p.status === 'success');
  let isDeploy = proc.env.findIndex(e => e === 'DEPLOY') !== -1 ? true : false;

  if (!isDeploy || succeded.length === testProcesses.length) {
    return startJobProcess(proc);
  } else if (queuedOrRunning.length) {
    // give some time (5s) to check again other processes
    return timer(5000).pipe(map(() => jobProcesses.next(proc)));
  } else {
    return from(stopJob(proc.job_id));
  }
}

export function startJobProcess(proc: JobProcess): Observable<{}> {
  return new Observable(observer => {
    (async () => {
      try {
        const repository = await getRepositoryByBuildId(proc.build_id);
        let envs = envVars.generate(proc);
        let secureVarirables = false;
        repository.variables.forEach(v => {
          if (!!v.encrypted) {
            secureVarirables = true;
            envVars.set(envs, v.name, decrypt(v.value), true);
          } else {
            envVars.set(envs, v.name, v.value);
          }
        });

        envVars.set(envs, 'ABSTRUSE_SECURE_ENV_VARS', secureVarirables);

        let jobTimeout = config.jobTimeout ? config.jobTimeout * 1000 : 3600000;
        let idleTimeout = config.idleTimeout ? config.idleTimeout * 1000 : 3600000;

        buildSub[proc.job_id] = startBuildProcess(proc, envs, jobTimeout, idleTimeout)
          .subscribe(event => {
            let msg: JobProcessEvent = {
              build_id: proc.build_id,
              job_id: proc.job_id,
              type: event.type,
              data: event.data
            };
            terminalEvents.next(msg);
            if (event.data && event.type === 'data') {
              proc.log += event.data;
            } else if (event.data && event.type === 'exposed ports') {
              proc.exposed_ports = event.data;
            } else if (event.type === 'container') {
              let ev: JobProcessEvent = {
                type: 'process',
                build_id: proc.build_id,
                job_id: proc.job_id,
                data: event.data
              };
              jobEvents.next(ev);
            } else if (event.type === 'exit') {
              proc.log += event.data;
              observer.complete();
            }
          }, err => {
            let msg_1: LogMessageType = {
              message: typeof err === 'object' ? `[error]: ${JSON.stringify(err)}` : `[error]: ${err}`, type: 'error', notify: false
            };
            jobFailed(proc, msg_1)
              .then(() => observer.complete());
          }, () => {
            jobSucceded(proc)
              .then(() => observer.complete());
          });

        const runId = await dbJob.getLastRunId(proc.job_id);
        let time = new Date();
        let data = { id: runId, start_time: time, end_time: null, status: 'running', log: '' };
        await dbJobRuns.updateJobRun(data);

        let jobRunData = {
          type: 'process',
          build_id: proc.build_id,
          job_id: proc.job_id,
          data: 'job started',
          additionalData: time.getTime()
        };
        jobEvents.next(jobRunData);
      } catch (err) {
        let msg_2: LogMessageType = {
          message: typeof err === 'object' ? `[error]: ${JSON.stringify(err)}` : `[error]: ${err}`, type: 'error', notify: false
        };
        logger.next(msg_2);
      }
    })();
  });
}

export async function restartJob(jobId: number): Promise<void> {
  let time = new Date();
  let process = processes.find(p => p.job_id === Number(jobId));
  if (process && process.debug) {
    process.debug = false;
    jobEvents.next({
      type: 'process',
      build_id: process.build_id,
      job_id: jobId,
      data: 'job failed',
      additionalData: time.getTime()
    });

    jobEvents.next({
      type: 'debug',
      build_id: process.build_id,
      job_id: jobId,
      data: 'false'
    });
  }

  try {
    await stopJob(jobId);
    const lastRun = await dbJob.getLastRun(jobId);
    await dbJobRuns.insertJobRun({
      start_time: time,
      end_time: null,
      status: 'queued',
      log: '',
      build_run_id: lastRun.build_run_id,
      job_id: jobId
    });

    await queueJob(jobId);
    const job = await dbJob.getJob(jobId);

    jobEvents.next({
      type: 'process',
      build_id: job.builds_id,
      job_id: job.id,
      data: 'job restarted',
      additionalData: time.getTime()
    });
    const build = await getBuild(job.builds_id);
    return await sendPendingStatus(build, build.id);
  } catch (err) {
    let msg: LogMessageType = {
      message: typeof err === 'object' ? `[error]: ${JSON.stringify(err)}` : `[error]: ${err}`, type: 'error', notify: false
    };
    logger.next(msg);
  }
}

export async function stopJob(jobId: number): Promise<void> {
  let time = new Date();
  let process = processes.find(p => p.job_id === Number(jobId));
  if (process && process.debug) {
    process.debug = false;
    jobEvents.next({
      type: 'process',
      build_id: process.build_id,
      job_id: jobId,
      data: 'job failed',
      additionalData: time.getTime()
    });

    jobEvents.next({
      type: 'debug',
      build_id: process.build_id,
      job_id: jobId,
      data: 'false'
    });
  }

  let job = await dbJob.getJob(jobId);

  try {
    await killContainer(`abstruse_${job.builds_id}_${jobId}`);

    const status = await getBuildStatus(job.builds_id);
    const build = await getBuild(job.builds_id);

    try {
      await updateBuild({ id: build.id, end_time: time });
      const id = await getLastRunId(build.id);
      await updateBuildRun({ id: id, end_time: time });

      if (status === 'success') {
        await sendSuccessStatus(build, build.id);
        jobEvents.next({
          type: 'process',
          build_id: build.id,
          data: 'build succeeded',
          additionalData: time.getTime()
        });
      } else if (status === 'failed') {
        await sendFailureStatus(build, build.id);
        jobEvents.next({
          type: 'process',
          build_id: build.id,
          data: 'build failed',
          additionalData: time.getTime()
        });
      }
    } catch (err) {
      let msg: LogMessageType = {
        message: typeof err === 'object' ? `[error]: ${JSON.stringify(err)}` : `[error]: ${err}`,
        type: 'error',
        notify: false
      };
      logger.next(msg);
    }
  } catch (err) {
    let msg: LogMessageType = {
      message: typeof err === 'object' ? `[error]: ${JSON.stringify(err)}` : `[error]: ${err}`, type: 'error', notify: false
    };
    logger.next(msg);
  }

  try {
    const runId = await dbJob.getLastRunId(jobId);
    const jobRun = await dbJobRuns.getRun(runId);
    if (!jobRun.end_time) {
      return dbJobRuns.updateJobRun({ id: jobRun.id, end_time: time, status: 'failed' });
    }

    job = await dbJob.getJob(jobId);
    let data = {
      type: 'process',
      build_id: job.builds_id,
      job_id: job.id,
      data: 'job stopped',
      additionalData: time.getTime()
    };
    jobEvents.next(data);

    if (buildSub[jobId]) {
      buildSub[jobId].unsubscribe();
      delete buildSub[jobId];
    }
  } catch (err) {
    let msg: LogMessageType = {
      message: typeof err === 'object' ? `[error]: ${JSON.stringify(err)}` : `[error]: ${err}`, type: 'error', notify: false
    };
    logger.next(msg);
  }
}

export function debugJob(jobId: number, debug: boolean): Promise<void> {
  return new Promise((resolve, reject) => {
    let time = new Date();
    let process = processes.find(p => p.job_id === Number(jobId));
    process.debug = debug;

    if (debug) {
      buildSub[jobId].unsubscribe();
      delete buildSub[jobId];

      let msg: JobProcessEvent = {
        build_id: process.build_id,
        job_id: Number(jobId),
        type: 'data',
        data: `[exectime]: stopped`
      };

      terminalEvents.next(msg);
      process.log += `[exectime]: stopped`;
    }
  });
}

export async function startBuild(data: any, buildConfig?: any): Promise<any> {
  try {
    let cfg: JobsAndEnv[];
    let repoId = data.repositories_id;
    let pr = null;
    let sha = null;
    let branch = null;
    let buildData = null;

    const repository = await getRepositoryOnly(data.repositories_id);
    let isGithub = repository.github_id ? true : false;
    let isBitbucket = repository.bitbucket_id ? true : false;
    let isGitlab = repository.gitlab_id  ? true : false;
    let isGogs = repository.gogs_id ? true : false;

    if (isGithub) {
      if (data.data.pull_request) {
        pr = data.data.pull_request.number;
        sha = data.data.pull_request.head.sha;
        branch = data.data.pull_request.base.ref;
      } else {
        sha = data.data.after || data.data.sha;
        if (data.data && data.data.ref) {
          branch = data.data.ref.replace('refs/heads/', '');
        }
      }
    } else if (isBitbucket) {
      if (data.data.push) {
        let push = data.data.push.changes[data.data.push.changes.length - 1];
        let commit = push.commits[push.commits.length - 1];
        sha = commit.hash;
        branch = push.new.type === 'branch' ? push.new.name : 'master';
      } else if (data.data.pullrequest) {
        pr = data.data.pullrequest.id;
        sha = data.data.pullrequest.source.commit.hash;
        branch = data.data.pullrequest.source.branch.name;
      } else if (data.hash) {
        sha = data.data.hash;
      }
    } else if (isGitlab) {
      if (data.data.name) {
        sha = data.data.commit.id;
        branch = data.data.name;
      }
    }

    branch = branch || repository.default_branch || 'master';

    let repo = {
      clone_url: repository.clone_url,
      branch: branch,
      pr: pr,
      sha: sha,
      access_token: repository.access_token || null,
      type: repository.repository_provider
    };

    if (buildConfig) {
      cfg = buildConfig;
    } else {
      cfg = await getRemoteParsedConfig(repo);
    }

    Object.assign(data, {
      branch,
      pr,
      parsed_config: JSON.stringify(cfg)
    });

    const build = await insertBuild(data);
    data = Object.assign(data, { build_id: build.id });
    delete data.repositories_id;
    delete data.pr;
    delete data.parsed_config;
    await insertBuildRun(data);

    buildData = await getBuild(data.build_id);
    await sendPendingStatus(buildData, buildData.id);

    for (const c of cfg) {
      let dataJob = null;

      const job = await dbJob.insertJob({ data: JSON.stringify(c), builds_id: data.build_id });
      dataJob = job;
      const lastRunId = await getLastRunId(data.build_id);
      const jobRun = {
        start_time: new Date,
        status: 'queued',
        build_run_id: lastRunId,
        job_id: dataJob.id
      };
      await dbJobRuns.insertJobRun(jobRun);
      await queueJob(dataJob.id);
    }

    jobEvents.next({
      type: 'process',
      build_id: data.build_id,
      repository_id: repoId,
      data: 'build added',
      additionalData: null
    });

    const deprcatedBuilds = await getDepracatedBuilds(buildData);
    await Promise.all(deprcatedBuilds.map(x => stopBuild(x)));

    return { buildId: buildData.id };
  } catch (err) {
    let msg: LogMessageType = {
      message: typeof err === 'object' ? `[error]: ${JSON.stringify(err)}` : `[error]: ${err}`, type: 'error', notify: false
    };
    logger.next(msg);
  }
}

export async function restartBuild(buildId: number): Promise<any> {
  let time = new Date();
  let buildData;
  let accessToken;

  try {
    await stopBuild(buildId);
    const build = await getBuild(buildId);
    buildData = build;
    accessToken = buildData.repository.access_token || null;
    let jobs = buildData.jobs;
    buildData.start_time = time;
    buildData.end_time = null;

    try {
      await updateBuild(buildData);
      buildData.build_id = buildId;

      const buildRun = await insertBuildRun(buildData);
      await Promise.all(jobs.map(job => {
        dbJobRuns.insertJobRun({
          start_time: time,
          end_time: null,
          status: 'queued',
          log: '',
          build_run_id: buildRun.id,
          job_id: job.id
        });
      }));

      for (const curr of jobs) {
        await stopJob(curr.id);
        await queueJob(curr.id);
      }

      const build_1 = await getBuild(buildId);
      await sendPendingStatus(build_1, build_1.id);

      jobEvents.next({
        type: 'process',
        build_id: buildId,
        data: 'build restarted',
        additionalData: time.getTime()
      });
    } catch (err) {
      let msg: LogMessageType = {
        message: typeof err === 'object' ? `[error]: ${JSON.stringify(err)}` : `[error]: ${err}`, type: 'error', notify: false
      };
      logger.next(msg);
    }
  } catch (err_1) {
    let msg_1: LogMessageType = {
      message: typeof err_1 === 'object' ? `[error]: ${JSON.stringify(err_1)}` : `[error]: ${err_1}`, type: 'error', notify: false
    };
    logger.next(msg_1);
  }
}

export async function stopBuild(buildId: number): Promise<any> {
  try {
    const build = await getBuild(buildId);
    for (const current of build.jobs) {
      await stopJob(current.id);
    }
  } catch (err) {
    let msg: LogMessageType = {
      message: typeof err === 'object' ? `[error]: ${JSON.stringify(err)}` : `[error]: ${err}`, type: 'error', notify: false
    };
    logger.next(msg);
  }
}

async function queueJob(jobId: number): Promise<void> {
  const job = await dbJob.getJob(jobId);
  const build = await getBuild(job.builds_id);
  const requestData = { branch: build.branch, pr: build.pr, data: build.data };
  let data = JSON.parse(job.data);
  let jobProcess: JobProcess = {
    build_id: job.builds_id,
    job_id: jobId,
    status: 'queued',
    requestData: requestData,
    commands: data.commands,
    cache: data.cache || null,
    repo_name: job.build.repository.full_name || null,
    branch: job.build.branch || null,
    env: data.env,
    image_name: data.image,
    exposed_ports: null,
    log: '',
    debug: false
  };
  jobProcesses.next(jobProcess);
  jobEvents.next({
    type: 'process',
    build_id: job.builds_id,
    job_id: job.id,
    data: 'job queued'
  });
}

async function jobSucceded(proc: JobProcess): Promise<any> {
  proc.status = 'success';
  let time = new Date();
  try {
    const runId = await dbJob.getLastRunId(proc.job_id);
    let data = {
      id: runId,
      end_time: time,
      status: 'success',
      log: proc.log
    };
    await dbJobRuns.updateJobRun(data);
    const status = await getBuildStatus(proc.build_id);
    if (status === 'success') {
      await updateBuild({ id: proc.build_id, end_time: time });
      const id = await getLastRunId(proc.build_id);
      await updateBuildRun({ id: id, end_time: time });
      const build = await getBuild(proc.build_id);
      await sendSuccessStatus(build, build.id);
      jobEvents.next({
        type: 'process',
        build_id: proc.build_id,
        data: 'build succeeded',
        additionalData: time.getTime()
      });
    } else if (status === 'failed') {
      const build_1 = await getBuild(proc.build_id);
      await updateBuild({ id: proc.build_id, end_time: time });
      const id_1 = await getLastRunId(proc.build_id);
      await updateBuildRun({ id: id_1, end_time: time });
      const build_2 = await getBuild(proc.build_id);
      await sendFailureStatus(build_2, build_2.id);
      jobEvents.next({
        type: 'process',
        build_id: proc.build_id,
        data: 'build failed',
        additionalData: time.getTime()
      });
    }
    jobEvents.next({
      type: 'process',
      build_id: proc.build_id,
      job_id: proc.job_id,
      data: 'job succeded',
      additionalData: time.getTime()
    });
  } catch (err) {
    let msg: LogMessageType = {
      message: typeof err === 'object' ? `[error]: ${JSON.stringify(err)}` : `[error]: ${err}`, type: 'error', notify: false
    };
    logger.next(msg);
    jobEvents.next({
      type: 'process',
      build_id: proc.build_id,
      job_id: proc.job_id,
      data: 'job failed',
      additionalData: time.getTime()
    });
    const id_1_1 = await getLastRunId(proc.build_id);
    return await updateBuildRun({ id: id_1_1, end_time: time });
  }
}

async function jobFailed(proc: JobProcess, msg?: LogMessageType): Promise<any> {
  await Promise.resolve();
  proc.status = 'errored';
  let time = new Date();
  if (msg) {
    logger.next(msg);
  }
  try {
    const runId = await dbJob.getLastRunId(proc.job_id);
    let data = {
      id: runId,
      end_time: time,
      status: 'failed',
      log: proc.log
    };
    const build = await dbJobRuns.updateJobRun(data);
    const id = await updateBuild({ id: proc.build_id, end_time: time });
    await updateBuildRun({ id: id, end_time: time });
    const build_1 = await getBuild(proc.build_id);
    await sendFailureStatus(build_1, build_1.id);
    jobEvents.next({
      type: 'process',
      build_id: proc.build_id,
      data: 'build failed',
      additionalData: time.getTime()
    });
    jobEvents.next({
      type: 'process',
      build_id: proc.build_id,
      job_id: proc.job_id,
      data: 'job failed',
      additionalData: time.getTime()
    });
  } catch (err) {
    let logMessage: LogMessageType = {
      message: typeof err === 'object' ? `[error]: ${JSON.stringify(err)}` : `[error]: ${err}`, type: 'error', notify: false
    };
    logger.next(logMessage);
  }
}
