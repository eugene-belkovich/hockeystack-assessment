const ProccesorService = require('./services/proccesor.service');
const DomainRepository = require('./repository/domain.repository');
const Domain = require('./model/Domain.model');
const HubspotService = require('./services/hubspot.service');
const QueueService = require('./services/queue.service');

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
    const q = QueueService.createQueue(domain, actions, account.hubId);

    await runInParallel([
      ProccesorService.processContacts(domain, account.hubId, q),
      ProccesorService.processCompanies(domain, account.hubId, q),
      ProccesorService.processMeetings(domain, account.hubId, q)
    ]);

    try {
      console.log('Queue: START drain queue');
      await QueueService.drainQueue(domain, actions, q, account.hubId);
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
