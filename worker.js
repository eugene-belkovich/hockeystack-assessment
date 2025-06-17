const {queue} = require('async');
const _ = require('lodash');

const ProccesorService = require('./proccesor.service');
const DomainRepository = require('./domain.repository');
const Domain = require('./Domain');
const HubspotService = require('./hubspot.service');

const QUEUE_TASKS_MAX = 2000;
const QUEUE_CONCURRENCY = 100;

const createQueue = (domain, actions, hubId) =>
  queue(async (action, callback) => {
    actions.push(action);

    if (actions.length > QUEUE_TASKS_MAX) {
      console.log('Actions: START inserting actions to database', {apiKey: domain.apiKey, count: actions.length});

      const copyOfActions = _.cloneDeep(actions);
      actions.splice(0, actions.length);

      await DomainRepository.saveActions(hubId, copyOfActions);
      console.log('Actions: END inserting actions to database', {apiKey: domain.apiKey, count: actions.length});
    }

    callback();
  }, QUEUE_CONCURRENCY);

const drainQueue = async (domain, actions, q, hubId) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    await DomainRepository.saveActions(hubId, actions);
  }

  return true;
};

const runInParallel = async jobs => {
  try {
    await Promise.all(jobs);
  } catch (err) {
    console.log(err);
  }
};

const pullDataFromHubspot = async () => {
  console.log('Worker: START pulling data from HubSpot');

  const domain = await Domain.findOne({});

  for (const account of domain.integrations.hubspot.accounts) {
    console.log('Account: START processing account', {apiKey: domain.apiKey, hubId: account.hubId});

    try {
      await HubspotService.refreshAccessToken(domain, account.hubId);
    } catch (err) {
      console.log(err, {apiKey: domain.apiKey, metadata: {operation: 'refreshAccessToken'}});
    }

    const actions = [];
    const q = createQueue(domain, actions, account.hubId);

    await runInParallel([
      ProccesorService.processContacts(domain, account.hubId, q),
      ProccesorService.processCompanies(domain, account.hubId, q),
      ProccesorService.processMeetings(domain, account.hubId, q)
    ]);

    try {
      console.log('Queue: START drain queue');
      await drainQueue(domain, actions, q, account.hubId);
    } catch (err) {
      console.log(err, {apiKey: domain.apiKey, metadata: {operation: 'drainQueue', hubId: account.hubId}});
    }
    console.log('Queue: END drain queue');

    await DomainRepository.saveDomain(domain);

    console.log('Account: END processing account', {apiKey: domain.apiKey, hubId: account.hubId});
  }

  console.log('Worker: END pulling data from HubSpot');
  process.exit();
};

module.exports = pullDataFromHubspot;
