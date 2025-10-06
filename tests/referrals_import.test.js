const assert = require('assert');

process.env.NODE_ENV = process.env.NODE_ENV || 'test';

const FakeClient = require('./fakeClient');
const {
  processBatch,
  setClientForTesting,
  normalizeDateForPg,
} = require('../pdgm');

(async () => {
  const client = new FakeClient({ tableName: 'referrals' });
  setClientForTesting(client);

  const tableColumns = ['Patient Name', 'Referral Date'];

  await processBatch(
    [
      { keys: ['1001'], rowData: ['Alice', normalizeDateForPg('01/01/2024')] },
      { keys: ['1001'], rowData: ['Alice Updated', normalizeDateForPg('02/01/2024')] },
      { keys: ['2002'], rowData: ['Bob', normalizeDateForPg('03/01/2024')] },
    ],
    'referrals',
    tableColumns,
    ['MRN']
  );

  const rows = client
    .getAllRows()
    .map(r => ({
      mrn: r.MRN,
      patient: r['Patient Name'],
      referralDate: r['Referral Date'],
    }))
    .sort((a, b) => a.mrn.localeCompare(b.mrn));

  assert.strictEqual(rows.length, 2, 'Expected two unique MRNs after upsert');
  assert.deepStrictEqual(
    rows.map(r => r.patient),
    ['Alice Updated', 'Bob'],
    'Latest record for MRN 1001 should be retained'
  );

  const deleteResult = await client.query(
    'DELETE FROM "referrals" WHERE "MRN" = ANY($1)',
    [['2002']]
  );

  assert.strictEqual(deleteResult.rowCount, 1, 'Expected one row deleted for MRN 2002');
  const remaining = client.getAllRows().map(r => r.MRN);
  assert.deepStrictEqual(remaining, ['1001'], 'MRN 2002 should be removed after deletion');

  console.log('✅ Referrals import helpers test passed');
})().catch(err => {
  console.error('❌ Referrals import helpers test failed');
  console.error(err);
  process.exit(1);
});
