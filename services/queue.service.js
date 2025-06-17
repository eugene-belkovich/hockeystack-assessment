const {queue} = require('async');
const _ = require('lodash');

const DomainRepository = require('../repository/domain.repository');

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

module.exports = {
  createQueue,
  drainQueue
};
