const assert = require('assert');

process.env.NODE_ENV = process.env.NODE_ENV || 'test';

const FakeClient = require('./fakeClient');
const {
  processBatch,
  setClientForTesting,
  normalizeDateForPg,
} = require('../pdgm');

(async () => {
  const client = new FakeClient();
  setClientForTesting(client);

  const tableColumns = ['Some Field'];
  const keyColumns = ['MRN', 'Period Start Date'];

  await processBatch(
    [
      { keys: ['12345', normalizeDateForPg('01/01/2024')], rowData: ['Alpha'] },
      { keys: ['12345', normalizeDateForPg('02/01/2024')], rowData: ['Beta'] },
    ],
    'pdgm',
    tableColumns,
    keyColumns
  );

  const rows = client
    .getAllRows()
    .map(r => ({
      mrn: r.MRN,
      periodStart: r['Period Start Date'],
      value: r['Some Field'],
    }))
    .sort((a, b) => a.periodStart.localeCompare(b.periodStart));

  assert.strictEqual(rows.length, 2, 'Expected two PDGM rows for distinct periods');
  assert.deepStrictEqual(
    rows.map(r => r.periodStart),
    ['2024-01-01', '2024-02-01'],
    'Period start dates should remain distinct'
  );
  assert.deepStrictEqual(rows.map(r => r.value), ['Alpha', 'Beta']);

  console.log('✅ PDGM composite key import test passed');
})().catch(err => {
  console.error('❌ PDGM composite key import test failed');
  console.error(err);
  process.exit(1);
});
