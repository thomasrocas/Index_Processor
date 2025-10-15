const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

process.env.NODE_ENV = process.env.NODE_ENV || 'test';

const FakeClient = require('./fakeClient');
const {
  importAdmissions,
  setClientForTesting,
} = require('../pdgm');

(async () => {
  const client = new FakeClient({
    tableName: 'admissions',
    columns: [
      'MRN',
      'Admission Date',
      'Patient Name',
      'Current Status',
    ],
    columnTypes: {
      MRN: 'character varying',
      'Admission Date': 'date',
      'Patient Name': 'text',
      'Current Status': 'text',
    },
  });

  client.storage.set('111|2024-01-01', {
    MRN: '111',
    'Admission Date': '2024-01-01',
    'Patient Name': 'Legacy Row',
    'Current Status': 'Stale',
  });

  setClientForTesting(client);

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'admissions-import-'));
  const filePath = path.join(tmpDir, 'admissions.csv');

  const csvContent = [
    'MRN,Admission Date,Action,Patient Name,Current Status',
    '123,01/01/2024,,Alice,Active',
    '456,02/01/2024,,Bob,Pending',
    '123,01/01/2024,,Alice Updated,Active',
    '456,02/01/2024,DELETE,Bob,Discharged',
    ',03/01/2024,,Missing,Invalid',
    '789,, ,Charlie,MissingDate',
  ].join('\n');

  fs.writeFileSync(filePath, csvContent, 'utf8');

  const summary = await importAdmissions(filePath, 'admissions');

  fs.rmSync(tmpDir, { recursive: true, force: true });

  assert.strictEqual(summary.processedRows, 6, 'Every CSV row should be processed');
  assert.strictEqual(summary.insertedOrUpdated, 3, 'Upsert count should reflect unique batches');
  assert.strictEqual(summary.deletedCount, 2, 'One explicit delete and one stale row removal');
  assert.strictEqual(summary.skippedRows, 2, 'Rows missing keys should be skipped');

  const rows = client
    .getAllRows()
    .map(r => ({
      key: `${r.MRN}|${r['Admission Date']}`,
      name: r['Patient Name'],
    }))
    .sort((a, b) => a.key.localeCompare(b.key));

  assert.deepStrictEqual(
    rows,
    [
      { key: '123|2024-01-01', name: 'Alice Updated' },
    ],
    'Importer should retain the latest admission per MRN/date and remove stale data'
  );

  console.log('✅ Admissions importer test passed');
})().catch(err => {
  console.error('❌ Admissions importer test failed');
  console.error(err);
  process.exit(1);
});

