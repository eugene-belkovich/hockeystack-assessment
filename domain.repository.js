const Domain = require('./Domain');
const {EntityTypeEnum} = require('./enum');

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

module.exports = {saveLastPulledDate};
