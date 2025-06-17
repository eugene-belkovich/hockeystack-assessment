const Domain = require('../model/Domain');
const {EntityTypeEnum} = require('../enum');

const saveDomain = async domain => {
  try {
    const update = domain.toObject();
    delete update._id;

    const result = await Domain.findOneAndUpdate({}, update, {new: true, upsert: false});

    if (!result) {
      throw new Error(`Domain with ID ${domain._id} not found`);
    }

    return result;
  } catch (err) {
    console.error('DomainRepository: saveDomain failed:', err);
    throw err;
  }
};

const saveActions = async (hubId, actions) => {
  if (!Array.isArray(actions) || actions.length === 0) return;

  try {
    const result = await Domain.updateOne(
      {'integrations.hubspot.accounts.hubId': hubId},
      {$push: {'integrations.hubspot.accounts.$[elem].actions': {$each: actions}}},
      {arrayFilters: [{'elem.hubId': hubId}]}
    );

    if (result?.matchedCount === 0) {
      throw new Error(`No matching domain/account for hubId=${hubId}`);
    }

    if (result.acknowledged !== true) {
      console.error('Error: actions not saved, acknowledged is false');
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

module.exports = {saveLastPulledDate, saveActions, saveDomain};
