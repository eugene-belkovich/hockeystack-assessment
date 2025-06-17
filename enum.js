const OperatorEnum = {
  GreaterOrEqual: 'GTE',
  LessOrEqual: 'LTE'
};

const EntityTypeEnum = {
  Contacts: 'contacts',
  Companies: 'companies',
  Meetings: 'meetings'
};

const DisallowedValuesUnion = [
  '[not provided]',
  'placeholder',
  '[[unknown]]',
  'not set',
  'not provided',
  'unknown',
  'undefined',
  '!$record',
  'n/a'
];

module.exports = {OperatorEnum, EntityTypeEnum, DisallowedValuesUnion};
