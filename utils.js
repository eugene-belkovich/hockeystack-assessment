const disallowedValues = [
  '[not provided]',
  'placeholder',
  '[[unknown]]',
  'not set',
  'not provided',
  'unknown',
  'undefined',
  'n/a'
];

const filterNullValuesFromObject = object =>
  Object.fromEntries(
    Object.entries(object).filter(([_, value]) => {
      if (value === null || value === '' || typeof value === 'undefined') return false;
      if (typeof value === 'string') {
        const lower = value.toLowerCase();
        if (disallowedValues.includes(lower)) return false;
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

const goal = actions => {
  // this is where the data will be written to the database
  // console.log(actions);
};

module.exports = {
  filterNullValuesFromObject,
  normalizePropertyName,
  goal
};
