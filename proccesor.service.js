const {filterNullValuesFromObject} = require('./utils');
const {EntityTypeEnum, OperatorEnum} = require('./enum');

const {retry} = require('./retry.service');
const DomainRepository = require('./domain.repository');
const HubspotService = require('./hubspot.service');

const LIMIT = 100;
const TOTAL_MIN = 0;
const TOTAL_MAX = 9900;

const generateLastModifiedDateFilter = (date, nowDate, propertyName = 'hs_lastmodifieddate') => {
  const lastModifiedDateFilter = date
    ? {
        filters: [
          {propertyName, operator: OperatorEnum.GreaterOrEqual, value: `${date.valueOf()}`},
          {propertyName, operator: OperatorEnum.LessOrEqual, value: `${nowDate.valueOf()}`}
        ]
      }
    : {};

  return lastModifiedDateFilter;
};

/**
 * Get recently modified companies as 100 companies per page
 */
const processCompanies = async (domain, hubId, q) => {
  console.log('Companies: START process companies');

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

    const searchResult = await retry(
      async () => {
        console.log('Company: START fetch company batch');
        const result = await HubspotService.hubspotClient.crm.companies.searchApi.doSearch(searchObject);
        console.log('Company: END fetch company batch');
        return result;
      },
      {
        onRetry: async error => {
          console.log('Retrying due to error:', error.message);
          if (new Date() > (HubspotService.tokenExpirationMap.get(hubId) || new Date(0))) {
            await HubspotService.refreshAccessToken(domain, hubId);
          }
        }
      }
    );

    if (!searchResult) throw new Error('Failed to fetch companies after all retries');

    const data = searchResult?.results || [];

    offsetObject.after = searchResult?.paging?.next?.after ? parseInt(searchResult.paging.next.after) : undefined;

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

      const isCreated = !lastPulledDate || new Date(company.createdAt) > lastPulledDate;

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

  await DomainRepository.saveLastPulledDate(hubId, EntityTypeEnum.Companies, now);

  console.log('Companies: END process companies');
  return true;
};

/**
 * Get recently modified contacts as 100 contacts per page
 */
const processContacts = async (domain, hubId, q) => {
  console.log('Contacts: START process contacts');

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

    const searchResult = await retry(
      async () => {
        console.log('Contact: START fetch contact batch');
        const result = await HubspotService.hubspotClient.crm.contacts.searchApi.doSearch(searchObject);
        console.log('Contact: END fetch contact batch');
        return result;
      },
      {
        onRetry: async error => {
          console.log('Retrying due to error:', error.message);
          if (new Date() > (HubspotService.tokenExpirationMap.get(hubId) || new Date(0))) {
            await HubspotService.refreshAccessToken(domain, hubId);
          }
        }
      }
    );

    if (!searchResult) throw new Error('Failed to fetch contacts after all retries');

    const data = searchResult.results || [];

    offsetObject.after = parseInt(searchResult.paging?.next?.after);
    const contactIds = data.map(contact => contact.id);

    // contact to company association
    const contactsToAssociate = contactIds;
    const companyAssociationsResults =
      (
        await (
          await HubspotService.hubspotClient.apiRequest({
            method: 'post',
            path: '/crm/v3/associations/CONTACTS/COMPANIES/batch/read',
            body: {inputs: contactsToAssociate.map(contactId => ({id: contactId}))}
          })
        ).json()
      )?.results || [];

    const processedContactIds = new Set();
    const companyAssociations = Object.fromEntries(
      companyAssociationsResults
        .map(association => {
          if (association.from && !processedContactIds.has(association.from.id)) {
            processedContactIds.add(association.from.id);
            return [association.from.id, association.to?.[0]?.id];
          }
          return false;
        })
        .filter(x => x)
    );

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

  await DomainRepository.saveLastPulledDate(hubId, EntityTypeEnum.Contacts, now);

  console.log('Contacts: END process contacts');
  return true;
};

/**
 * Get recently modified meetings as 100 meetings per page
 */
const processMeetings = async (domain, hubId, q) => {
  console.log('Meetings: START processing meetings');

  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const lastPulledDate = new Date(account.lastPulledDates.meetings);

  const now = new Date();

  let hasMore = true;
  const offsetObject = {};

  while (hasMore) {
    const lastModifiedDate = offsetObject.lastModifiedDate || lastPulledDate;
    const lastModifiedDateFilter = generateLastModifiedDateFilter(lastModifiedDate, now, 'hs_lastmodifieddate');
    const searchObject = {
      filterGroups: [lastModifiedDateFilter],
      sorts: [{propertyName: 'hs_lastmodifieddate', direction: 'ASCENDING'}],
      properties: ['hs_meeting_title', 'hs_timestamp', 'createdate', 'lastmodifieddate'],
      limit: LIMIT,
      after: offsetObject.after
    };

    const searchResult = await retry(
      async () => {
        console.log('Meeting: START fetch contact batch');
        const result = await HubspotService.hubspotClient.crm.objects.searchApi.doSearch('meetings', searchObject);
        console.log('Meeting: END fetch contact batch');
        return result;
      },
      {
        onRetry: async error => {
          console.log('Retrying due to error:', error.message);
          if (new Date() > (HubspotService.tokenExpirationMap.get(hubId) || new Date(0))) {
            await HubspotService.refreshAccessToken(domain, hubId);
          }
        }
      }
    );

    if (!searchResult) throw new Error('Failed to fetch meetings for the 4th time. Aborting.');
    const data = searchResult.results || [];

    offsetObject.after = parseInt(searchResult.paging?.next?.after);

    const meetingIds = data.map(meeting => meeting.id);

    const meetingToContactsAssociationsResults =
      (
        await (
          await HubspotService.hubspotClient.apiRequest({
            method: 'post',
            path: '/crm/v3/associations/MEETINGS/CONTACTS/batch/read',
            body: {inputs: meetingIds.map(meetingId => ({id: meetingId}))}
          })
        ).json()
      )?.results || [];

    const processedMeetingIds = new Set();
    const meetingToContactsAssociations = new Map();

    meetingToContactsAssociationsResults.forEach(association => {
      if (association.from && !processedMeetingIds.has(association.from.id)) {
        processedMeetingIds.add(association.from.id);
        if (association.to?.[0]?.id) {
          meetingToContactsAssociations.set(association.from.id, association.to[0].id);
        }
      }
    });

    const contactIds = Array.from(new Set(Object.values(meetingToContactsAssociations)));

    const contactsResults =
      (
        await (
          await HubspotService.hubspotClient.apiRequest({
            method: 'post',
            path: '/crm/v3/objects/contacts/batch/read',
            body: {inputs: contactIds.map(id => ({id}))}
          })
        ).json()
      )?.results || [];

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

  await DomainRepository.saveLastPulledDate(hubId, EntityTypeEnum.Meetings, now);

  console.log('Meetings: END processing meetings');
  return true;
};

module.exports = {processCompanies, processContacts, processMeetings};
