const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

process.env.NODE_ENV = process.env.NODE_ENV || 'test';

const FakeClient = require('./fakeClient');
const {
  importAdmissionIdTable,
  setClientForTesting,
} = require('../pdgm');

(async () => {
  const client = new FakeClient({
    tableName: 'raw_data',
    columns: ['Admission ID', 'Some Date', 'Amount', 'Notes'],
    columnTypes: {
      'Admission ID': 'character varying',
      'Some Date': 'date',
      Amount: 'numeric',
      Notes: 'text',
    },
  });

  client.storage.set('456', {
    'Admission ID': '456',
    'Some Date': '2024-01-01',
    Amount: '99.99',
    Notes: 'Existing row',
  });

  setClientForTesting(client);

  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'admission-import-'));
  const filePath = path.join(tmpDir, 'raw_data.csv');

  const csvContent = [
    'Admission ID,Action,Some Date,Amount,Notes',
    '123,,01/02/2024,$100.50,Initial load',
    ',,01/03/2024,200,Missing ID',
    '456,DELETE,02/04/2024,($50.00),Remove existing',
    '123,,03/05/2024,$150.75,Updated amount',
    'ABC123,,04/06/2024,10,Alpha entry',
  ].join('\n');

  fs.writeFileSync(filePath, csvContent, 'utf8');

  const summary = await importAdmissionIdTable({
    filePath,
    tableName: 'raw_data',
    ensureConstraint: async () => {},
  });

  fs.rmSync(tmpDir, { recursive: true, force: true });

  assert.strictEqual(summary.processedRows, 5, 'Should process every CSV row');
  assert.strictEqual(summary.insertedOrUpdated, 3, 'Should upsert three records');
  assert.strictEqual(summary.deletedCount, 1, 'Should delete matching Admission IDs');
  assert.strictEqual(summary.skippedRows, 1, 'Rows missing Admission ID should be skipped');

  const rows = client
    .getAllRows()
    .map(r => ({
      admissionId: r['Admission ID'],
      amount: r['Amount'],
      notes: r['Notes'],
    }))
    .sort((a, b) => a.admissionId.localeCompare(b.admissionId));

  assert.deepStrictEqual(
    rows,
    [
      { admissionId: '123', amount: '150.75', notes: 'Updated amount' },
      { admissionId: 'ABC123', amount: '10', notes: 'Alpha entry' },
    ],
    'Importer should upsert latest rows and remove deleted ones'
  );

  console.log('✅ Admission ID table importer test passed');
})().catch(err => {
  console.error('❌ Admission ID table importer test failed');
  console.error(err);
  process.exit(1);
});
