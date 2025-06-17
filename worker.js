const hubspot = require('@hubspot/api-client');
const {queue} = require('async');
const _ = require('lodash');

const {filterNullValuesFromObject, goal} = require('./utils');
const Domain = require('./Domain');

const hubspotClient = new hubspot.Client({accessToken: ''});

const LIMIT = 100;
const TOTAL_MIN = 0;
const TOTAL_MAX = 9900;
const TRY_COUNT_MAX = 4;
const TIMEOUT_5_SEC = 5000;
const QUEUE_TASKS_MAX = 2000;
const QUEUE_CONCURRENCY = 100000000;

let expirationDate;

const OperatorEnum = {
  GreaterOrEqual: 'GTE',
  LessOrEqual: 'LTE'
};

const generateLastModifiedDateFilter = (date, nowDate, propertyName = 'hs_lastmodifieddate') => {
  const lastModifiedDateFilter = date ?
    {
      filters: [
        {propertyName, operator: OperatorEnum.GreaterOrEqual, value: `${date.valueOf()}`},
        {propertyName, operator: OperatorEnum.LessOrEqual, value: `${nowDate.valueOf()}`}
      ]
    } :
    {};

  return lastModifiedDateFilter;
};

const saveDomain = async domain => {
  // disable this for testing purposes
  return;

  domain.markModified('integrations.hubspot.accounts');
  await domain.save();
};

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId, tryCount) => {
  const {HUBSPOT_CID, HUBSPOT_CS} = process.env;
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const {accessToken, refreshToken} = account;

  return hubspotClient.oauth.tokensApi
    .createToken('refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, refreshToken)
    .then(async result => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

      hubspotClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      return true;
    });
};

/**
 * Get recently modified companies as 100 companies per page
 */
const processCompanies = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.companies);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now);
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING'}],
      properties: [
        'name',
        'domain',
        'country',
        'industry',
        'description',
        'annualrevenue',
        'numberofemployees',
        'hs_lead_status'
      ],
      limit: LIMIT,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount <= TRY_COUNT_MAX) {
      try {
        searchResult = await hubspotClient.crm.companies.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, TIMEOUT_5_SEC * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error('Failed to fetch companies for the 4th time. Aborting.');

    const data = searchResult?.results || [];
    offsetObject.after = parseInt(searchResult?.paging?.next?.after);

    console.log('fetch company batch');

    data.forEach(company => {
      if (!company.properties) return;

      const actionTemplate = {
        includeInAnalytics: 0,
        companyProperties: {
          company_id: company.id,
          company_domain: company.properties.domain,
          company_industry: company.properties.industry
        }
      };

      const isCreated = !lastPulledDate || (new Date(company.createdAt) > lastPulledDate);

      q.push({
        actionName: isCreated ? 'Company Created' : 'Company Updated',
        actionDate: new Date(isCreated ? company.createdAt : company.updatedAt) - 2000,
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= TOTAL_MAX) {
      offsetObject.after = TOTAL_MIN;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.companies = now;
  await saveDomain(domain);

  return true;
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.contacts);
  const now = new Date();

  let hasMore = true;
  const offsetObject = {};

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'lastmodifieddate');
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{propertyName: 'lastmodifieddate', direction: 'ASCENDING'}],
      properties: [
        'firstname',
        'lastname',
        'jobtitle',
        'email',
        'hubspotscore',
        'hs_lead_status',
        'hs_analytics_source',
        'hs_latest_source'
      ],
      limit: LIMIT,
      after: offsetObject.after
    };

    let searchResult = {};

    let tryCount = 0;
    while (tryCount <= TRY_COUNT_MAX) {
      try {
        searchResult = await hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
        break;
      } catch (err) {
        tryCount++;

        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);

        await new Promise((resolve, reject) => setTimeout(resolve, TIMEOUT_5_SEC * Math.pow(2, tryCount)));
      }
    }

    if (!searchResult) throw new Error('Failed to fetch contacts for the 4th time. Aborting.');

    const data = searchResult.results || [];

    console.log('fetch contact batch');

    offsetObject.after = parseInt(searchResult.paging?.next?.after);
    const contactIds = data.map(contact => contact.id);

    // contact to company association
    const contactsToAssociate = contactIds;
    const companyAssociationsResults = (await (await hubspotClient.apiRequest({
      method: 'post',
      path: '/crm/v3/associations/CONTACTS/COMPANIES/batch/read',
      body: {inputs: contactsToAssociate.map(contactId => ({id: contactId}))}
    })).json())?.results || [];

    const companyAssociations = Object.fromEntries(companyAssociationsResults.map(a => {
      if (a.from) {
        contactsToAssociate.splice(contactsToAssociate.indexOf(a.from.id), 1);
        return [a.from.id, a.to[0].id];
      } else return false;
    }).filter(x => x));

    data.forEach(contact => {
      if (!contact.properties || !contact.properties.email) return;

      const companyId = companyAssociations[contact.id];

      const isCreated = new Date(contact.createdAt) > lastPulledDate;

      const userProperties = {
        company_id: companyId,
        contact_name: ((contact.properties.firstname || '') + ' ' + (contact.properties.lastname || '')).trim(),
        contact_title: contact.properties.jobtitle,
        contact_source: contact.properties.hs_analytics_source,
        contact_status: contact.properties.hs_lead_status,
        contact_score: parseInt(contact.properties.hubspotscore) || 0
      };

      const actionTemplate = {
        includeInAnalytics: 0,
        identity: contact.properties.email,
        userProperties: filterNullValuesFromObject(userProperties)
      };

      q.push({
        actionName: isCreated ? 'Contact Created' : 'Contact Updated',
        actionDate: new Date(isCreated ? contact.createdAt : contact.updatedAt),
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= TOTAL_MAX) {
      offsetObject.after = TOTAL_MIN;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }

  account.lastPulledDates.contacts = now;
  await saveDomain(domain);
  return true;
};

/**
 * Get recently modified meetings as 100 meetings per page
 */
const processMeetings = async (domain, hubId, q) => {
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account?.lastPulledDates?.meetings || 0);

  const now = new Date();

  let hasMore = true;
  const offsetObject = {};

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'hs_lastmodifieddate');
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING'}],
      properties: [
        'hs_meeting_title',
        'hs_timestamp',
        'createdate',
        'lastmodifieddate'
      ],
      limit: LIMIT,
      after: offsetObject.after
    };

    let searchResult = {};
    let tryCount = 0;
    while (tryCount <= TRY_COUNT_MAX) {
      try {
        searchResult = await hubspotClient.crm.objects.searchApi.doSearch('meetings', searchObject);
        break;
      } catch (err) {
        tryCount++;
        if (new Date() > expirationDate) await refreshAccessToken(domain, hubId);
        await new Promise(resolve => setTimeout(resolve, TIMEOUT_5_SEC * Math.pow(2, tryCount)));
      }
    }
    if (!searchResult) throw new Error('Failed to fetch meetings for the 4th time. Aborting.');
    const data = searchResult.results || [];

    offsetObject.after = parseInt(searchResult.paging?.next?.after);
    console.log('fetch meeting batch');

    const meetingIds = data.map(meeting => meeting.id);

    const meetingToContactsAssociationsResults = (await (await hubspotClient.apiRequest({
      method: 'post',
      path: '/crm/v3/associations/MEETINGS/CONTACTS/batch/read',
      body: {inputs: meetingIds.map(meetingId => ({id: meetingId}))}
    })).json())?.results || [];

    const meetingToContactsAssociations = Object.fromEntries(meetingToContactsAssociationsResults.map(a => {
      if (a.from) {
        meetingIds.splice(meetingIds.indexOf(a.from.id), 1);
        return [a.from.id, a.to[0].id];
      } else return false;
    }).filter(x => x));

    const contactIds = Array.from(new Set(Object.values(meetingToContactsAssociations)));

    const contactsResults = (await (await hubspotClient.apiRequest({
      method: 'post',
      path: '/crm/v3/objects/contacts/batch/read',
      body: {inputs: contactIds.map(id => ({id}))}
    })).json())?.results || [];

    const contactIdToEmailMap = Object.fromEntries(
      contactsResults.map(contact => [contact.id, contact.properties?.email])
    );

    data.forEach(meeting => {
      if (!meeting.id) return;
      const contactId = meetingToContactsAssociations[meeting.id] || null;
      const email = contactIdToEmailMap[contactId] || null;

      const isCreated = new Date(meeting.createdAt) > lastPulledDate;
      const actionTemplate = {
        includeInAnalytics: 0,
        identity: email,
        meetingProperties: filterNullValuesFromObject({
          meeting_id: meeting.id,
          meeting_title: meeting.properties?.hs_meeting_title,
          meeting_timestamp: meeting.properties?.hs_timestamp,
          meeting_createdate: meeting.properties?.createdate,
          meeting_lastmodifieddate: meeting.properties?.lastmodifieddate
        })
      };
      q.push({
        actionName: isCreated ? 'Meeting Created' : 'Meeting Updated',
        actionDate: new Date(isCreated ? meeting.createdAt : meeting.updatedAt),
        ...actionTemplate
      });
    });

    if (!offsetObject?.after) {
      hasMore = false;
      break;
    } else if (offsetObject?.after >= TOTAL_MAX) {
      offsetObject.after = TOTAL_MIN;
      offsetObject.lastModifiedDate = new Date(data[data.length - 1].updatedAt).valueOf();
    }
  }
  account.lastPulledDates.meetings = now;
  await saveDomain(domain);

  return true;
};

const createQueue = (domain, actions) => queue(async (action, callback) => {
  actions.push(action);

  if (actions.length > QUEUE_TASKS_MAX) {
    console.log('inserting actions to database', {apiKey: domain.apiKey, count: actions.length});

    const copyOfActions = _.cloneDeep(actions);
    actions.splice(0, actions.length);

    goal(copyOfActions);
  }

  callback();
}, QUEUE_CONCURRENCY);

const drainQueue = async (domain, actions, q) => {
  if (q.length() > 0) await q.drain();

  if (actions.length > 0) {
    goal(actions);
  }

  return true;
};

const pullDataFromHubspot = async () => {
  console.log('start pulling data from HubSpot');

  const domain = await Domain.findOne({});

  for (const account of domain.integrations.hubspot.accounts) {
    console.log('start processing account');

    try {
      await refreshAccessToken(domain, account.hubId);
    } catch (err) {
      console.log(err, {apiKey: domain.apiKey, metadata: {operation: 'refreshAccessToken'}});
    }

    const actions = [];
    const q = createQueue(domain, actions);

    try {
      await processContacts(domain, account.hubId, q);
      console.log('process contacts');
    } catch (err) {
      console.log(err, {apiKey: domain.apiKey, metadata: {operation: 'processContacts', hubId: account.hubId}});
    }

    try {
      await processCompanies(domain, account.hubId, q);
      console.log('process companies');
    } catch (err) {
      console.log(err, {apiKey: domain.apiKey, metadata: {operation: 'processCompanies', hubId: account.hubId}});
    }

    try {
      console.log('Meetings: START processing meetings');
      await processMeetings(domain, account.hubId, q);
    } catch (err) {
      console.log(err, {apiKey: domain.apiKey, metadata: {operation: 'processMeetings', hubId: account.hubId}});
    }
    console.log('Meetings: END processing meetings');

    try {
      await drainQueue(domain, actions, q);
      console.log('drain queue');
    } catch (err) {
      console.log(err, {apiKey: domain.apiKey, metadata: {operation: 'drainQueue', hubId: account.hubId}});
    }

    await saveDomain(domain);

    console.log('finish processing account');
  }

  process.exit();
};

module.exports = pullDataFromHubspot;
