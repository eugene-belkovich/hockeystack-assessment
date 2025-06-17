const Domain = require('./Domain');
const {EntityTypeEnum} = require('./enum');

const saveActions = async (hubId, actions) => {
  if (!Array.isArray(actions) || actions.length === 0) return;

  try {
    const result = await Domain.updateOne(
      {'integrations.hubspot.accounts.hubId': hubId},
      {$push: {'integrations.hubspot.accounts.$[elem].actions': {$each: actions}}}
    );

    if (result.matchedCount === 0) {
      console.warn(`Update did not match any documents for hubId: ${hubId}`);
    }

    return result;
  } catch (err) {
    console.error('DomainRepository: saveActions failed:', err);
    throw err;
  }
};

const saveLastPulledDate = async (hubId, entityType, lastPulledDate) => {
  try {
    if (!Object.values(EntityTypeEnum).includes(entityType)) {
      throw new Error(`Unsupported entityType: ${entityType}`);
    }

    await Domain.updateOne(
      {'integrations.hubspot.accounts.hubId': hubId},
      {$set: {[`integrations.hubspot.accounts.$.lastPulledDates.${entityType}`]: lastPulledDate}}
    );
  } catch (err) {
    console.error(`DomainRepository: saveLastPulledDate failed:`, err);
    throw err;
  }
};

module.exports = {saveLastPulledDate, saveActions};
