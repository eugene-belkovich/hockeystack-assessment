const hubspot = require('@hubspot/api-client');

const tokenExpirationMap = new Map();

const hubspotClient = new hubspot.Client({accessToken: ''});

/**
 * Get access token from HubSpot
 */
const refreshAccessToken = async (domain, hubId) => {
  const {HUBSPOT_CID, HUBSPOT_CS} = process.env;
  const account = domain.integrations.hubspot.accounts.find(account => account.hubId === hubId);
  const {accessToken, refreshToken} = account;

  return hubspotClient.oauth.tokensApi
    .createToken('refresh_token', undefined, undefined, HUBSPOT_CID, HUBSPOT_CS, refreshToken)
    .then(async result => {
      const body = result.body ? result.body : result;

      const newAccessToken = body.accessToken;
      const expirationDate = new Date(body.expiresIn * 1000 + new Date().getTime());

      tokenExpirationMap.set(hubId, expirationDate);

      hubspotClient.setAccessToken(newAccessToken);
      if (newAccessToken !== accessToken) {
        account.accessToken = newAccessToken;
      }

      return true;
    });
};

module.exports = {refreshAccessToken, tokenExpirationMap, hubspotClient};
