const {DisallowedValuesUnion} = require('./enum');

const filterNullValuesFromObject = object =>
  Object.fromEntries(
    Object.entries(object).filter(([_, value]) => {
      if (value === null || value === '' || typeof value === 'undefined') return false;
      if (typeof value === 'string') {
        const lower = value.toLowerCase();
        if (DisallowedValuesUnion.includes(lower)) return false;
        if (lower.includes('!$record')) return false;
      }
      return true;
    })
  );

const normalizePropertyName = key =>
  key
    .toLowerCase()
    .replace(/__c$/, '')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_');

module.exports = {
  filterNullValuesFromObject,
  normalizePropertyName
};
