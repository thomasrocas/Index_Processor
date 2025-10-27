const isTestEnv = process.env.NODE_ENV === 'test';

const express = isTestEnv ? null : require('express');
const { Client } = isTestEnv ? { Client: class {} } : require('pg');
const multer = isTestEnv ? null : require('multer');
const fs = require('fs');
const path = require('path');
const { parse } = isTestEnv ? require('./tests/csv-parse-stub') : require('csv-parse');
const { stringify } = isTestEnv ? require('./tests/csv-stringify-stub') : require('csv-stringify');
const copyFrom = isTestEnv ? null : require('pg-copy-streams').from;
const { Transform } = require('stream');

const app = isTestEnv ? createTestAppStub() : express();
const PORT = 3000;

let client = new Client({
  user: 'postgres',
  host: 'localhost',
  database: 'clinicalvisits',
  password: '@DbAdmin@',
  port: 5432,
});

if (process.env.NODE_ENV !== 'test') {
  client.connect()
    .then(() => console.log('✅ Connected to PostgreSQL!'))
    .catch(err => console.error('❌ PostgreSQL connection error', err));
}

const upload = isTestEnv ? createTestUploadStub() : multer({ dest: 'uploads/' });
if (!isTestEnv) {
  app.use(express.static('public'));
  app.use(express.urlencoded({ extended: true }));
}

const importProgressStore = new Map();
const PROGRESS_CLEANUP_MS = 10 * 60 * 1000; // 10 minutes

function generateImportId() {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

function createImportSession(tableName) {
  const id = generateImportId();
  const entry = {
    id,
    tableName,
    status: 'queued',
    processedRows: 0,
    totalRows: 0,
    percentage: 0,
    summary: null,
    message: '',
    error: null,
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
  importProgressStore.set(id, entry);
  return entry;
}

function updateImportSession(id, updates = {}) {
  const entry = importProgressStore.get(id);
  if (!entry) return null;

  if (typeof updates.totalRows === 'number') {
    entry.totalRows = updates.totalRows;
  }
  if (typeof updates.processedRows === 'number') {
    entry.processedRows = updates.processedRows;
  }
  if (typeof updates.status === 'string') {
    entry.status = updates.status;
  }
  if (typeof updates.message === 'string') {
    entry.message = updates.message;
  }
  if (updates.summary !== undefined) {
    entry.summary = updates.summary;
  }
  if (updates.error !== undefined) {
    entry.error = updates.error;
  }

  entry.percentage = entry.totalRows > 0
    ? Math.min(100, Math.round((entry.processedRows / entry.totalRows) * 100))
    : 0;
  entry.updatedAt = Date.now();
  return entry;
}

function completeImportSession(id, summary, message) {
  const entry = updateImportSession(id, {
    status: 'completed',
    processedRows: summary?.processedRows ?? 0,
    summary,
    message,
  });
  if (entry) {
    entry.completedAt = Date.now();
    scheduleImportCleanup(id);
  }
}

function failImportSession(id, error) {
  const entry = updateImportSession(id, {
    status: 'failed',
    error: error instanceof Error ? error.message : String(error),
  });
  if (entry) {
    entry.completedAt = Date.now();
    scheduleImportCleanup(id);
  }
}

function scheduleImportCleanup(id) {
  if (isTestEnv) return; // tests rely on deterministic state
  setTimeout(() => importProgressStore.delete(id), PROGRESS_CLEANUP_MS).unref?.();
}

function getImportSessionSnapshot(id) {
  const entry = importProgressStore.get(id);
  if (!entry) return null;
  return { ...entry };
}

async function countCsvRecords(filePath) {
  let count = 0;
  try {
    const parser = fs.createReadStream(filePath).pipe(parse(CSV_OPTS_TOLERANT));
    for await (const _row of parser) {
      count++;
    }
  } catch (err) {
    throw err;
  }
  return count;
}

function setClientForTesting(testClient) {
  client = testClient;
}

function createTestAppStub() {
  return {
    use: () => {},
    post: () => {},
    get: () => {},
  };
}

function createTestUploadStub() {
  return {
    single: () => (req, _res, next) => {
      if (typeof next === 'function') next();
    },
  };
}

// ------------------ Helpers ------------------
function toNullIfEmpty(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s === '' ? null : s;
}

function normalizeIdentifier(name) {
  return (name ?? '')
    .toString()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '');
}

const CSV_OPTS_TOLERANT = {
  columns: true,
  trim: true,
  skip_empty_lines: true,
  relax_column_count: true,   // ✅ accept short/long rows
  relax_quotes: true,         // ✅ handle stray quotes
  bom: true,                  // ✅ handle UTF-8 BOM
  skip_lines_with_error: true // ✅ silently skip broken lines
};

// Normalize common date strings to YYYY-MM-DD.
// If a range like "10/30/2022 - 11/5/2022" appears, take the first date.
function normalizeDateForPg(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  if (s === '') return null;
  const m = s.match(/(\d{1,2})\/(\d{1,2})\/(\d{2,4})/);
  if (m) {
    const mm = String(parseInt(m[1], 10)).padStart(2,'0');
    const dd = String(parseInt(m[2], 10)).padStart(2,'0');
    let yy = m[3];
    let yyyy = yy.length === 2 ? (parseInt(yy,10) < 70 ? '20' + yy : '19' + yy) : yy;
    return `${yyyy}-${mm}-${dd}`;
  }
  const d = new Date(s);
  if (!isNaN(d)) {
    const yyyy = d.getFullYear();
    const mm = String(d.getMonth()+1).padStart(2,'0');
    const dd = String(d.getDate()).padStart(2,'0');
    return `${yyyy}-${mm}-${dd}`;
  }
  return null;
}

async function ensureBilledArAgingConstraint() {
  const check = await client.query(`
    SELECT tc.constraint_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_column_usage ccu
      ON tc.constraint_name = ccu.constraint_name AND tc.table_name = ccu.table_name
    WHERE tc.table_schema = 'public'
      AND tc.table_name = 'billed_ar_aging'
      AND tc.constraint_type = 'UNIQUE'
      AND ccu.column_name IN ('Service Dates','Med Rec #','Invoice Num')
    GROUP BY tc.constraint_name
    HAVING COUNT(*) = 3
  `);
  if (check.rowCount === 0) {
    await client.query(`
      ALTER TABLE "billed_ar_aging"
      ADD CONSTRAINT billed_ar_aging_unique UNIQUE("Service Dates", "Med Rec #", "Invoice Num")
    `);
  }
}

async function ensureQueueManagerConstraint() {
  const check = await client.query(`
    SELECT tc.constraint_name
    FROM information_schema.table_constraints tc
    JOIN information_schema.constraint_column_usage ccu
      ON tc.constraint_name = ccu.constraint_name AND tc.table_name = ccu.table_name
    WHERE tc.table_schema = 'public'
      AND tc.table_name = 'queue_manager'
      AND tc.constraint_type IN ('UNIQUE','PRIMARY KEY')
      AND ccu.column_name = 'QueueID'
  `);
  if (check.rowCount === 0) {
    await client.query(`
      ALTER TABLE "queue_manager"
      ADD CONSTRAINT queue_manager_queueid_uniq UNIQUE("QueueID")
    `);
  }
}


async function ensureActiveAgencyConstraint() {
  await client.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = 'active_agency'
          AND indexname = 'active_agency_admission_id_uniq'
      ) THEN
        EXECUTE 'CREATE UNIQUE INDEX active_agency_admission_id_uniq
                 ON public.active_agency("Admission ID")';
      END IF;
    END$$;
  `);
}


async function ensureRawDataConstraint() {
  await client.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = 'raw_data'
          AND indexname = 'raw_data_admission_id_uniq'
      ) THEN
        EXECUTE 'CREATE UNIQUE INDEX raw_data_admission_id_uniq
                 ON public.raw_data("Admission ID")';
      END IF;
    END$$;
  `);
}


async function ensureReferralsConstraint() {
  await client.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = 'referrals'
          AND indexname = 'referrals_mrn_uniq'
      ) THEN
        EXECUTE 'CREATE UNIQUE INDEX referrals_mrn_uniq
                 ON public.referrals("MRN")';
      END IF;
    END$$;
  `);
}

async function ensureAdmissionsConstraint() {
  await client.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = 'admissions'
          AND indexname = 'admissions_mrn_admission_date_uniq'
      ) THEN
        EXECUTE 'CREATE UNIQUE INDEX admissions_mrn_admission_date_uniq
                 ON public.admissions("MRN", "Admission Date")';
      END IF;
    END$$;
  `);
}


async function ensurePdgmConstraint() {
  const colRes = await client.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'pdgm'
  `);

  const columnNames = colRes.rows.map(r => r.column_name);
  const lookup = new Map();
  columnNames.forEach(name => {
    lookup.set(normalizeIdentifier(name), name);
  });

  const mrnColumn = lookup.get('mrn');
  const periodColumn =
    lookup.get('periodstart') || lookup.get('periodstartdate');

  if (!mrnColumn || !periodColumn) {
    throw new Error('PDGM table must contain MRN and Period Start columns');
  }

  await client.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS pdgm_mrn_period_start_uniq
    ON public.pdgm("${mrnColumn}", "${periodColumn}")
  `);

  return { keyColumns: [mrnColumn, periodColumn] };
}



// ---------- Logging Helpers ----------
const LOG_FILE = path.join(__dirname, 'maintable_log.txt');

async function ensureLogFileExists() {
  try {
    await fs.promises.access(LOG_FILE, fs.constants.F_OK);
  } catch {
    await fs.promises.writeFile(
      LOG_FILE,
      "Datetime | User | Processed | Inserted/Updated | Deleted | Skipped\n",
      { encoding: "utf8" }
    );
  }
}

function resolveOsUser() {
  const envUser =
    process.env.SUDO_USER ||
    process.env.USER ||
    process.env.USERNAME ||
    process.env.LOGNAME;
  if (envUser) return envUser;

  try {
    return require('os').userInfo().username;
  } catch {
    return null;
  }
}

function resolveActor(req) {
  return (
    (req.body && (req.body.username || req.body.user || req.body.createdBy)) ||
    (req.user && (req.user.username || req.user.email || req.user.id)) ||
    req.headers["x-forwarded-user"] ||
    resolveOsUser() ||
    req.ip ||
    "unknown"
  );
}

async function appendImportLog(actor, summary, funcName) {
  await ensureLogFileExists();
  const line = [
    new Date().toISOString(),
    actor,
    funcName,
    summary.processedRows,
    summary.insertedOrUpdated,
    summary.deletedCount,
    summary.skippedRows
  ].join(" | ") + "\n";

  await fs.promises.appendFile(LOG_FILE, line, { encoding: "utf8" });
}



function buildSummaryMessage(label, summary) {
  if (!summary) return '';
  return [
    `✅ ${label} Done!`,
    `Processed Rows: ${summary.processedRows}`,
    `Inserted/Updated: ${summary.insertedOrUpdated}`,
    `Deleted Rows: ${summary.deletedCount}`,
    `Skipped Rows: ${summary.skippedRows}`,
  ].join('\n');
}

const IMPORT_CONFIG = {
  casemanager: {
    runner: (filePath, tableName, options) => importCaseManager(filePath, tableName, null, options),
    logName: 'importCaseManager',
    formatMessage: (summary) => buildSummaryMessage('CaseManager', summary),
  },
  maintable: {
    runner: (filePath, tableName, options) => importMainTable(filePath, tableName, options),
    logName: 'importMainTable',
    formatMessage: (summary) => buildSummaryMessage('MainTable', summary),
  },
  unduplicatedpatients: {
    runner: (filePath, tableName, options) => importUnduplicatedPatients(filePath, tableName, options),
    logName: 'importUnduplicatedPatients',
    formatMessage: (summary) => buildSummaryMessage('UnduplicatedPatients', summary),
  },
  admissions: {
    runner: (filePath, tableName, options) => importAdmissions(filePath, tableName, options),
    logName: 'importAdmissions',
    formatMessage: (summary) => buildSummaryMessage('Admissions', summary),
  },
  active_agency: {
    runner: (filePath, tableName, options) => importActiveAgency(filePath, tableName, options),
    logName: 'importActiveAgency',
    formatMessage: (summary) => buildSummaryMessage('Import ActiveAgency', summary),
  },
  raw_data: {
    runner: (filePath, tableName, options) => importRawData(filePath, tableName, options),
    logName: 'importRawData',
    formatMessage: (summary) => buildSummaryMessage('Import RawData', summary),
  },
  pdgm: {
    runner: (filePath, tableName, options) => importPdgm(filePath, tableName, options),
    logName: 'importPdgm',
    formatMessage: (summary) => buildSummaryMessage('Import PDGM', summary),
  },
  billed_ar_aging: {
    runner: (filePath, tableName, options) => importBilledArAging(filePath, tableName, options),
    logName: 'importBilledArAging',
    afterImport: async () => {
      await client.query('SELECT public.refresh_all_rollups()');
    },
    formatMessage: (summary) => buildSummaryMessage('Billed AR Aging', summary),
  },
  queue_manager: {
    runner: (filePath, tableName, options) => importQueueManager(filePath, tableName, options),
    logName: 'importQueueManager',
    afterImport: async () => {
      await client.query('SELECT public.refresh_all_rollups()');
    },
    formatMessage: (summary) => buildSummaryMessage('Queue Manager', summary),
  },
  referrals: {
    runner: (filePath, tableName, options) => importReferrals(filePath, tableName, options),
    logName: 'importReferrals',
    formatMessage: (summary) => buildSummaryMessage('Referrals', summary),
  },
};

// ------------------ ROUTES ------------------
app.post('/upload', upload.single('csvfile'), async (req, res) => {
  const tableNameRaw = req.body.tableName;
  const tableName = (tableNameRaw ?? '').toString().trim().toLowerCase();
  const filePath = req.file?.path;

  if (!filePath) return res.status(400).json({ error: 'No file uploaded.' });
  if (!tableName) {
    fs.unlinkSync(filePath);
    return res.status(400).json({ error: 'No table selected.' });
  }

  const config = IMPORT_CONFIG[tableName];
  if (!config) {
    fs.unlinkSync(filePath);
    return res.status(400).json({ error: '❌ Unknown table selected.' });
  }

  const session = createImportSession(tableName);
  res.status(202).json({ importId: session.id, status: session.status });

  (async () => {
    try {
      let totalRows = 0;
      try {
        totalRows = await countCsvRecords(filePath);
        updateImportSession(session.id, { totalRows, status: 'importing', processedRows: 0 });
      } catch (countErr) {
        console.warn('⚠️ Failed to count CSV rows for progress tracking', countErr);
        updateImportSession(session.id, { status: 'importing', processedRows: 0 });
      }

      const onProgress = (processed) => updateImportSession(session.id, { processedRows: processed });
      const options = { onProgress, totalRows };
      let summary = await config.runner(filePath, tableName, options);

      if (!summary) {
        summary = {
          processedRows: totalRows,
          insertedOrUpdated: totalRows,
          deletedCount: 0,
          skippedRows: 0,
        };
      }

      updateImportSession(session.id, { processedRows: summary.processedRows });

      if (typeof config.afterImport === 'function') {
        await config.afterImport(summary, tableName);
      }

      const message = config.formatMessage
        ? config.formatMessage(summary, tableName)
        : buildSummaryMessage(tableName, summary);

      completeImportSession(session.id, summary, message);

      if (config.logName) {
        const actor = resolveActor(req);
        await appendImportLog(actor, summary, config.logName);
      }
    } catch (err) {
      console.error('❌ Import job error:', err);
      try {
        if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
      } catch (_) {}
      failImportSession(session.id, err);
    }
  })();
});

app.get('/progress/:id', (req, res) => {
  const snapshot = getImportSessionSnapshot(req.params.id);
  if (!snapshot) {
    return res.status(404).json({ status: 'not_found', importId: req.params.id });
  }
  return res.json(snapshot);
});

// ------------------ CASEMANAGER IMPORT ------------------
async function importCaseManager(filePath, tableName, res, options = {}) {
  const { onProgress } = options;
  try {
    const result = await client.query(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema = 'public' AND table_name = $1
      ORDER BY ordinal_position
    `, [tableName]);

    const tableColumns = result.rows.map(r => `"${r.column_name}"`);
    const columnTypes = {};
    result.rows.forEach(r => { columnTypes[r.column_name] = r.data_type; });

    await client.query(`TRUNCATE TABLE "${tableName}"`);

    function normalizeDate(v) {
      if (!v) return '';
      const d = new Date(v);
      return isNaN(d) ? '' : d.toISOString().split('T')[0];
    }

    const isDateCol = {};
    for (const [name, typ] of Object.entries(columnTypes)) {
      isDateCol[name] = /\bdate\b/i.test(typ);
    }

    let processedRows = 0;
    const reorderAndSanitize = new Transform({
      writableObjectMode: true,
      readableObjectMode: true,
      transform(record, _enc, cb) {
        processedRows++;
        if (typeof onProgress === 'function') onProgress(processedRows);

        const out = [];
        for (const col of tableColumns) {
          const plain = col.replace(/(^")|("$)/g, '');
          let v = record[plain] ?? '';
          if (isDateCol[plain]) v = normalizeDate(v);
          out.push(v);
        }
        cb(null, out);
      }
    });

    const copyStream = client.query(copyFrom(
      `COPY "${tableName}" (${tableColumns.join(',')}) FROM STDIN CSV NULL ''`
    ));

    const formattedStream = fs.createReadStream(filePath)
      .pipe(parse({ columns: true, trim: true }))
      .pipe(reorderAndSanitize)
      .pipe(stringify({ header: false }));

    const pipelinePromise = new Promise((resolve, reject) => {
      formattedStream.pipe(copyStream);
      copyStream.on('finish', resolve);
      copyStream.on('error', reject);
      formattedStream.on('error', reject);
    });

    await pipelinePromise;
    fs.unlinkSync(filePath);
    if (res) {
      res.send(`✅ CaseManager: Table "${tableName}" imported successfully!`);
    }

    return {
      processedRows,
      insertedOrUpdated: processedRows,
      deletedCount: 0,
      skippedRows: 0,
    };

  } catch (err) {
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    throw err;
  }
}

// ------------------ MAINTABLE IMPORT ------------------
async function importMainTable(filePath, tableName, options = {}) {
  const { onProgress } = options;
  const BATCH_SIZE = 500;
  let processedRows = 0, insertedOrUpdated = 0, deletedCount = 0;
  const skippedRows = [];
  const deleteIds = new Set();
  const csvVisitIds = new Set();
  let minDate = null, maxDate = null;
  let dateColumn = null;

  const result = await client.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1 AND is_generated = 'NEVER'
    ORDER BY ordinal_position
  `, [tableName]);

  const allColumns = result.rows.map(r => r.column_name);
  const tableColumns = allColumns.filter(c => c !== 'Visit ID');
  dateColumn = allColumns.find(c => /date/i.test(c));
  await client.query('BEGIN');

  try {
    let batch = [];
    const parser = fs.createReadStream(filePath).pipe(parse(CSV_OPTS_TOLERANT));

    for await (const row of parser) {
      processedRows++;
      if (typeof onProgress === 'function') onProgress(processedRows);
      const visitId = row['Visit ID'];
      const action = (row['Action'] || '').toUpperCase();
      const dateStr = dateColumn ? row[dateColumn] : null;
      if (dateStr) {
        const d = new Date(dateStr);
        if (!isNaN(d)) {
          if (!minDate || d < minDate) minDate = d;
          if (!maxDate || d > maxDate) maxDate = d;
        }
      }

      if (!visitId) {
        skippedRows.push({ row, reason: 'Missing Visit ID' });
        continue;
      }

      csvVisitIds.add(visitId);

      if (action === 'DELETE') {
        deleteIds.add(visitId);
        continue; // Skip upsert for deletions
      }

      const rowData = tableColumns.map(c => toNullIfEmpty(row[c]));
      batch.push({ keys: [visitId], rowData });
      if (batch.length >= BATCH_SIZE) {
        await processBatch(batch, tableName, tableColumns, ['Visit ID']);
        insertedOrUpdated += batch.length;
        batch = [];
      }
    }
    if (batch.length) {
      await processBatch(batch, tableName, tableColumns, ['Visit ID']);
      insertedOrUpdated += batch.length;
    }

    if (deleteIds.size) {
      const deleteRes = await client.query(
        `DELETE FROM "${tableName}" WHERE "Visit ID" = ANY($1)`,
        [Array.from(deleteIds)]
      );
      deletedCount += deleteRes.rowCount;
    }

    if (dateColumn && minDate && maxDate && csvVisitIds.size) {
      const deleteExistingRes = await client.query(
        `DELETE FROM "${tableName}" WHERE "${dateColumn}" BETWEEN $1 AND $2 AND "Visit ID" NOT IN (SELECT unnest($3::text[]))`,
        [minDate, maxDate, Array.from(csvVisitIds)]
      );
      deletedCount += deleteExistingRes.rowCount;
    }

    await client.query('COMMIT');
    fs.unlinkSync(filePath);

    return {
      processedRows,
      insertedOrUpdated,
      deletedCount,
      skippedRows: skippedRows.length,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    throw err;
  }
}

async function processBatch(batch, tableName, tableColumns, keyColumns) {
  const keys = Array.isArray(keyColumns) ? keyColumns : [keyColumns];
  const dedupedMap = new Map();
  for (const item of batch) {
    const keyString = item.keys.join('|');
    dedupedMap.set(keyString, { keys: item.keys, rowData: item.rowData });
  }

  const dedupedBatch = Array.from(dedupedMap.values());

  const values = [];
  let paramIdx = 1;
  const placeholders = dedupedBatch.map(b => {
    const rowPlaceholders = [];
    for (let i = 0; i < keys.length + tableColumns.length; i++) {
      rowPlaceholders.push(`$${paramIdx++}`);
    }
    values.push(...b.keys, ...b.rowData);
    return `(${rowPlaceholders.join(', ')})`;
  });

  const colNames = [...keys.map(c => `"${c}"`), ...tableColumns.map(c => `"${c}"`)];
  const updateSet = tableColumns.map(c => `"${c}" = EXCLUDED."${c}"`);
  const conflictCols = keys.map(c => `"${c}"`).join(', ');

  const query = `
    INSERT INTO "${tableName}" (${colNames.join(', ')})
    VALUES ${placeholders.join(', ')}
    ON CONFLICT (${conflictCols}) DO UPDATE SET ${updateSet.join(', ')}
  `;
  await client.query(query, values);
}

// ------------------ UNDUPLICATEDPATIENTS IMPORT ------------------
async function importUnduplicatedPatients(filePath, tableName, options = {}) {
  const { onProgress } = options;
  const BATCH_SIZE = 500;
  let processedRows = 0, insertedOrUpdated = 0, deletedCount = 0;
  const skippedRows = [];
  const deleteIds = new Set();
  const csvMrns = new Set();
  let minDate = null, maxDate = null;
  let dateColumn = null;

  const result = await client.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1 AND is_generated = 'NEVER'
    ORDER BY ordinal_position
  `, [tableName]);

  const allColumns = result.rows.map(r => r.column_name);
  const tableColumns = allColumns.filter(c => c !== 'MRN');
  dateColumn = allColumns.find(c => /date/i.test(c));
  await client.query('BEGIN');

  try {
    let batch = [];
    const parser = fs.createReadStream(filePath).pipe(parse(CSV_OPTS_TOLERANT));

    for await (const row of parser) {
      processedRows++;
      if (typeof onProgress === 'function') onProgress(processedRows);
      const mrn = row['MRN'];
      const action = (row['Action'] || '').toUpperCase();
      const dateStr = dateColumn ? row[dateColumn] : null;
      if (dateStr) {
        const d = new Date(dateStr);
        if (!isNaN(d)) {
          if (!minDate || d < minDate) minDate = d;
          if (!maxDate || d > maxDate) maxDate = d;
        }
      }

      if (!mrn) {
        skippedRows.push({ row, reason: 'Missing MRN' });
        continue;
      }

      csvMrns.add(mrn);

      if (action === 'DELETE') {
        deleteIds.add(mrn);
        continue;
      }

      const rowData = tableColumns.map(c => toNullIfEmpty(row[c]));
      batch.push({ keys: [mrn], rowData });
      if (batch.length >= BATCH_SIZE) {
        await processBatch(batch, tableName, tableColumns, ['MRN']);
        insertedOrUpdated += batch.length;
        batch = [];
      }
    }
    if (batch.length) {
      await processBatch(batch, tableName, tableColumns, ['MRN']);
      insertedOrUpdated += batch.length;
    }

    if (deleteIds.size) {
      const deleteRes = await client.query(
        `DELETE FROM "${tableName}" WHERE "MRN" = ANY($1)`,
        [Array.from(deleteIds)]
      );
      deletedCount += deleteRes.rowCount;
    }

    if (dateColumn && minDate && maxDate && csvMrns.size) {
      const deleteExistingRes = await client.query(
        `DELETE FROM "${tableName}" WHERE "${dateColumn}" BETWEEN $1 AND $2 AND "MRN" NOT IN (SELECT unnest($3::text[]))`,
        [minDate, maxDate, Array.from(csvMrns)]
      );
      deletedCount += deleteExistingRes.rowCount;
    }

    await client.query('COMMIT');
    fs.unlinkSync(filePath);

    return {
      processedRows,
      insertedOrUpdated,
      deletedCount,
      skippedRows: skippedRows.length,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    throw err;
  }
}

// ------------------ ADMISSIONS IMPORT ------------------
async function importAdmissions(filePath, tableName, options = {}) {
  const { onProgress } = options;
  const BATCH_SIZE = 500;
  const keyColumns = ['MRN', 'Admission Date'];
  let processedRows = 0, insertedOrUpdated = 0, deletedCount = 0;
  const skippedRows = [];
  const deleteKeys = new Set();
  const csvKeys = new Set();
  let minDate = null;
  let maxDate = null;

  await ensureAdmissionsConstraint();

  const result = await client.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1 AND is_generated = 'NEVER'
    ORDER BY ordinal_position
  `, [tableName]);

  const allColumns = result.rows.map(r => r.column_name);
  for (const col of keyColumns) {
    if (!allColumns.includes(col)) {
      throw new Error(`Admissions table must contain a "${col}" column`);
    }
  }

  const tableColumns = allColumns.filter(c => !keyColumns.includes(c));

  const typeRes = await client.query(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1
  `, [tableName]);

  const isDateCol = {};
  typeRes.rows.forEach(row => {
    const type = (row.data_type || '').toLowerCase();
    isDateCol[row.column_name] = type.includes('date') || type.includes('time');
  });

  await client.query('BEGIN');

  try {
    let batch = [];
    const parser = fs.createReadStream(filePath).pipe(parse(CSV_OPTS_TOLERANT));

    for await (const row of parser) {
      processedRows++;
      if (typeof onProgress === 'function') onProgress(processedRows);

      const rawMrn = row['MRN'];
      const mrn = toNullIfEmpty(rawMrn);
      const admissionDateRaw = row['Admission Date'];
      const admissionDate = normalizeDateForPg(admissionDateRaw);
      const action = (row['Action'] || '').toUpperCase();

      if (!mrn || !admissionDate) {
        skippedRows.push({ row, reason: 'Missing MRN or Admission Date' });
        continue;
      }

      const compositeKey = `${mrn}|${admissionDate}`;
      csvKeys.add(compositeKey);

      const dateObj = new Date(admissionDate);
      if (!isNaN(dateObj)) {
        if (!minDate || dateObj < minDate) minDate = dateObj;
        if (!maxDate || dateObj > maxDate) maxDate = dateObj;
      }

      if (action === 'DELETE') {
        deleteKeys.add(compositeKey);
        continue;
      }

      const rowData = tableColumns.map(column => {
        const value = row[column];
        return isDateCol[column] ? normalizeDateForPg(value) : toNullIfEmpty(value);
      });

      batch.push({ keys: [mrn, admissionDate], rowData });

      if (batch.length >= BATCH_SIZE) {
        await processBatch(batch, tableName, tableColumns, keyColumns);
        insertedOrUpdated += batch.length;
        batch = [];
      }
    }

    if (batch.length) {
      await processBatch(batch, tableName, tableColumns, keyColumns);
      insertedOrUpdated += batch.length;
    }

    const compositeExpr =
      `COALESCE("MRN"::text,'') || '|' || COALESCE(to_char("Admission Date",'YYYY-MM-DD'),'')`;

    if (deleteKeys.size) {
      const deleteRes = await client.query(
        `DELETE FROM "${tableName}" WHERE ${compositeExpr} = ANY($1)`,
        [Array.from(deleteKeys)]
      );
      deletedCount += deleteRes.rowCount;
    }

    if (minDate && maxDate && csvKeys.size) {
      const deleteExistingRes = await client.query(
        `DELETE FROM "${tableName}" WHERE "Admission Date" BETWEEN $1 AND $2 AND NOT (${compositeExpr} = ANY($3))`,
        [minDate, maxDate, Array.from(csvKeys)]
      );
      deletedCount += deleteExistingRes.rowCount;
    }

    await client.query('COMMIT');
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);

    return {
      processedRows,
      insertedOrUpdated,
      deletedCount,
      skippedRows: skippedRows.length,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    throw err;
  }
}

async function importReferrals(filePath, tableName, options = {}) {
  const { onProgress } = options;
  const BATCH_SIZE = 500;
  let processedRows = 0, insertedOrUpdated = 0, deletedCount = 0;
  const skippedRows = [];
  const deleteIds = new Set();
  const csvMrns = new Set();
  let minDate = null, maxDate = null;
  let dateColumn = null;

  await ensureReferralsConstraint();

  const result = await client.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1 AND is_generated = 'NEVER'
    ORDER BY ordinal_position
  `, [tableName]);

  const allColumns = result.rows.map(r => r.column_name);
  if (!allColumns.includes('MRN')) {
    throw new Error('Referrals table must contain an MRN column');
  }

  const tableColumns = allColumns.filter(c => c !== 'MRN');

  const typeRes = await client.query(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1
  `, [tableName]);

  const isDateCol = {};
  typeRes.rows.forEach(row => {
    const type = (row.data_type || '').toLowerCase();
    isDateCol[row.column_name] = type.includes('date') || type.includes('time');
  });

  dateColumn = tableColumns.find(c => /date/i.test(c) && isDateCol[c]) ||
               allColumns.find(c => isDateCol[c]) ||
               null;

  await client.query('BEGIN');

  try {
    let batch = [];
    const parser = fs.createReadStream(filePath).pipe(parse(CSV_OPTS_TOLERANT));

    for await (const row of parser) {
      processedRows++;
      if (typeof onProgress === 'function') onProgress(processedRows);

      const rawMrn = row['MRN'];
      const mrn = toNullIfEmpty(rawMrn);
      const action = (row['Action'] || '').toUpperCase();
      const dateStr = dateColumn
        ? (isDateCol[dateColumn] ? normalizeDateForPg(row[dateColumn]) : row[dateColumn])
        : null;

      if (dateStr) {
        const d = new Date(dateStr);
        if (!isNaN(d)) {
          if (!minDate || d < minDate) minDate = d;
          if (!maxDate || d > maxDate) maxDate = d;
        }
      }

      if (!mrn) {
        skippedRows.push({ row, reason: 'Missing MRN' });
        continue;
      }

      csvMrns.add(mrn);

      if (action === 'DELETE') {
        deleteIds.add(mrn);
        continue;
      }

      const rowData = tableColumns.map(column => {
        const value = row[column];
        return isDateCol[column] ? normalizeDateForPg(value) : toNullIfEmpty(value);
      });

      batch.push({ keys: [mrn], rowData });

      if (batch.length >= BATCH_SIZE) {
        await processBatch(batch, tableName, tableColumns, ['MRN']);
        insertedOrUpdated += batch.length;
        batch = [];
      }
    }

    if (batch.length) {
      await processBatch(batch, tableName, tableColumns, ['MRN']);
      insertedOrUpdated += batch.length;
    }

    if (deleteIds.size) {
      const deleteRes = await client.query(
        `DELETE FROM "${tableName}" WHERE "MRN" = ANY($1)`,
        [Array.from(deleteIds)]
      );
      deletedCount += deleteRes.rowCount;
    }

    if (dateColumn && minDate && maxDate && csvMrns.size) {
      const deleteExistingRes = await client.query(
        `DELETE FROM "${tableName}" WHERE "${dateColumn}" BETWEEN $1 AND $2 AND "MRN" NOT IN (SELECT unnest($3::text[]))`,
        [minDate, maxDate, Array.from(csvMrns)]
      );
      deletedCount += deleteExistingRes.rowCount;
    }

    await client.query('COMMIT');
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);

    return {
      processedRows,
      insertedOrUpdated,
      deletedCount,
      skippedRows: skippedRows.length,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    throw err;
  }
}

// ------------------ ADMISSION-ID IMPORT HELPERS ------------------

async function importAdmissionIdTable({ filePath, tableName, ensureConstraint, onProgress }) {
  const BATCH_SIZE = 500;
  let processedRows = 0;
  let insertedOrUpdated = 0;
  let deletedCount = 0;
  const skippedRows = [];
  const deleteIds = new Set();
  const csvIds = new Set();
  let minDate = null;
  let maxDate = null;

  if (ensureConstraint) {
    await ensureConstraint();
  }

  const result = await client.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1 AND is_generated = 'NEVER'
    ORDER BY ordinal_position
  `, [tableName]);

  const allColumns = result.rows.map(r => r.column_name);

  const typeRes = await client.query(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1
  `, [tableName]);

  const colTypes = {};
  typeRes.rows.forEach(r => {
    colTypes[r.column_name] = r.data_type;
  });

  const isDateCol = {};
  const numericColumnKinds = {};
  Object.keys(colTypes).forEach(c => {
    const t = (colTypes[c] || '').toLowerCase();
    const isDateLike = t.includes('date') || t.includes('time');
    isDateCol[c] = isDateLike;
    if (!isDateLike) {
      if (/\b(?:int|bigint|smallint)\b/.test(t)) {
        numericColumnKinds[c] = 'integer';
      } else if (/\b(?:numeric|decimal|double|real|money|float)\b/.test(t)) {
        numericColumnKinds[c] = 'decimal';
      }
    }
  });

  const keyColumn = 'Admission ID';
  const tableColumns = allColumns.filter(c => c !== keyColumn);
  const dateColumn = allColumns.find(c => /date/i.test(c));

  await client.query('BEGIN');

  try {
    let batch = [];
    const parser = fs.createReadStream(filePath).pipe(parse(CSV_OPTS_TOLERANT));

    let admissionHeader = null;

    function sanitizeColumnValue(columnName, rawValue) {
      if (!columnName) return toNullIfEmpty(rawValue);

      if (isDateCol[columnName]) {
        return normalizeDateForPg(rawValue);
      }

      const numericKind = numericColumnKinds[columnName];
      if (!numericKind) {
        return toNullIfEmpty(rawValue);
      }

      if (rawValue === null || rawValue === undefined) return null;

      let s = String(rawValue).trim();
      if (s === '') return null;

      let isNegative = false;
      if (s.startsWith('(') && s.endsWith(')')) {
        isNegative = true;
        s = s.slice(1, -1);
      }

      s = s.replace(/[\$,]/g, '').replace(/\s+/g, '');
      if (isNegative) s = '-' + s;

      if (numericKind === 'integer') {
        s = s.replace(/^\+/, '');
        if (!/^[+-]?\d+$/.test(s)) return null;
        return s;
      }

      s = s.replace(/^\+/, '');
      if (!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(s)) return null;
      if (s.startsWith('.')) s = '0' + s;
      if (s.startsWith('-.')) s = '-0' + s.slice(1);
      if (s.endsWith('.')) s = s + '0';
      return s;
    }

    for await (const row of parser) {
      if (!admissionHeader) {
        admissionHeader =
          Object.keys(row).find(
            k => k && k.toLowerCase().replace(/[\s\-]+/g, '_') === 'admission_id'
          ) || keyColumn;
      }

      processedRows++;
      if (typeof onProgress === 'function') onProgress(processedRows);

      const rawAdmissionId = admissionHeader ? row[admissionHeader] : row[keyColumn];
      const admissionId = sanitizeColumnValue(keyColumn, rawAdmissionId);
      const action = (row['Action'] || '').toUpperCase();

      const dateStr = dateColumn ? sanitizeColumnValue(dateColumn, row[dateColumn]) : null;
      if (dateStr) {
        const d = new Date(dateStr);
        if (!isNaN(d)) {
          if (!minDate || d < minDate) minDate = d;
          if (!maxDate || d > maxDate) maxDate = d;
        }
      }

      if (admissionId === null) {
        skippedRows.push({
          row,
          reason: 'Admission ID missing or invalid after normalization',
        });
        continue;
      }

      csvIds.add(admissionId);

      if (action === 'DELETE') {
        deleteIds.add(admissionId);
        continue;
      }

      const rowData = tableColumns.map(c => sanitizeColumnValue(c, row[c]));
      batch.push({ keys: [admissionId], rowData });

      if (batch.length >= BATCH_SIZE) {
        await processBatch(batch, tableName, tableColumns, [keyColumn]);
        insertedOrUpdated += batch.length;
        batch = [];
      }
    }

    if (batch.length) {
      await processBatch(batch, tableName, tableColumns, [keyColumn]);
      insertedOrUpdated += batch.length;
    }

    if (deleteIds.size) {
      const deleteRes = await client.query(
        `DELETE FROM "${tableName}" WHERE "${keyColumn}" = ANY($1)`,
        [Array.from(deleteIds)]
      );
      deletedCount += deleteRes.rowCount;
    }

    if (dateColumn && minDate && maxDate && csvIds.size) {
      const deleteExistingRes = await client.query(
        `DELETE FROM "${tableName}" WHERE "${dateColumn}" BETWEEN $1 AND $2 AND NOT ("${keyColumn}" = ANY($3))`,
        [minDate, maxDate, Array.from(csvIds)]
      );
      deletedCount += deleteExistingRes.rowCount;
    }

    await client.query('COMMIT');
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);

    return {
      processedRows,
      insertedOrUpdated,
      deletedCount,
      skippedRows: skippedRows.length,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    throw err;
  }
}

async function importActiveAgency(filePath, tableName, options = {}) {
  return importAdmissionIdTable({
    filePath,
    tableName,
    ensureConstraint: ensureActiveAgencyConstraint,
    onProgress: options.onProgress,
  });
}

async function importRawData(filePath, tableName, options = {}) {
  return importAdmissionIdTable({
    filePath,
    tableName,
    ensureConstraint: ensureRawDataConstraint,
    onProgress: options.onProgress,
  });
}

// ------------------ PDGM IMPORT ------------------
async function importPdgm(filePath, tableName, options = {}) {
  const { onProgress } = options;
  const BATCH_SIZE = 500;
  let processedRows = 0, insertedOrUpdated = 0, deletedCount = 0;
  const skippedRows = [];
  const deleteKeys = new Set();
  const csvKeys = new Set();
  let minDate = null, maxDate = null;
  let dateColumn = null;

  const { keyColumns } = await ensurePdgmConstraint();
  const keyColumnSet = new Set(keyColumns);

  const result = await client.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1 AND is_generated = 'NEVER'
    ORDER BY ordinal_position
  `, [tableName]);

  const allColumns = result.rows.map(r => r.column_name);
  const tableColumns = allColumns.filter(c => !keyColumnSet.has(c));

  const typeRes = await client.query(`
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1
    ORDER BY ordinal_position
  `, [tableName]);

  const colTypes = {};
  typeRes.rows.forEach(r => { colTypes[r.column_name] = r.data_type; });

  const isDateCol = {};
  Object.keys(colTypes).forEach(c => {
    const t = (colTypes[c] || '').toLowerCase();
    isDateCol[c] = t.includes('date') || t.includes('time');
  });

  const periodKeyColumn = keyColumns.find(c => /period/.test(normalizeIdentifier(c)));
  const preferredDateOrder = [
    periodKeyColumn,
    'Period Start',
    'Period Start Date',
    'Admitted Date',
    'SOC',
    'Discharge Date',
    'DOB'
  ].filter(Boolean);
  dateColumn = preferredDateOrder.find(c => allColumns.includes(c) && isDateCol[c]) ||
               allColumns.find(c => /date/i.test(c)) || null;

  const compositeExpr = keyColumns
    .map(c => `COALESCE("${c}"::text,'')`)
    .join(` || '|' || `);

  await client.query('BEGIN');

  try {
    let batch = [];
    const parser = fs.createReadStream(filePath).pipe(parse(CSV_OPTS_TOLERANT));
    let headerMap = null;

    const headerFallbacks = new Map([
      ['periodstart', ['periodstartdate']],
      ['periodstartdate', ['periodstart']],
    ]);

    const resolveHeader = columnName => {
      const normalized = normalizeIdentifier(columnName);
      if (headerMap?.has(normalized)) return headerMap.get(normalized);
      const fallbacks = headerFallbacks.get(normalized);
      if (fallbacks) {
        for (const alt of fallbacks) {
          if (headerMap?.has(alt)) return headerMap.get(alt);
        }
      }
      return columnName;
    };

    for await (const row of parser) {
      if (!headerMap) {
        headerMap = new Map();
        Object.keys(row).forEach(k => {
          headerMap.set(normalizeIdentifier(k), k);
        });
      }

      processedRows++;
      if (typeof onProgress === 'function') onProgress(processedRows);

      const actionRaw = row[resolveHeader('Action')];
      const action = (actionRaw || '').toUpperCase();

      let dateStr = null;
      if (dateColumn) {
        const rawDate = row[resolveHeader(dateColumn)];
        dateStr = isDateCol[dateColumn] ? normalizeDateForPg(rawDate) : rawDate;
        if (dateStr) {
          const d = new Date(dateStr);
          if (!isNaN(d)) {
            if (!minDate || d < minDate) minDate = d;
            if (!maxDate || d > maxDate) maxDate = d;
          }
        }
      }

      const keyValues = keyColumns.map(col => {
        const raw = row[resolveHeader(col)];
        return isDateCol[col] ? normalizeDateForPg(raw) : toNullIfEmpty(raw);
      });

      if (keyValues.some(v => v === null || v === undefined || v === '')) {
        skippedRows.push({ row, reason: 'Missing key columns' });
        continue;
      }

      const compositeKey = keyValues.map(v => (v ?? '')).join('|');
      csvKeys.add(compositeKey);

      if (action === 'DELETE') {
        deleteKeys.add(compositeKey);
        continue;
      }

      const rowData = tableColumns.map(col => {
        const raw = row[resolveHeader(col)];
        return isDateCol[col] ? normalizeDateForPg(raw) : toNullIfEmpty(raw);
      });

      batch.push({ keys: keyValues, rowData });

      if (batch.length >= BATCH_SIZE) {
        await processBatch(batch, tableName, tableColumns, keyColumns);
        insertedOrUpdated += batch.length;
        batch = [];
      }
    }

    if (batch.length) {
      await processBatch(batch, tableName, tableColumns, keyColumns);
      insertedOrUpdated += batch.length;
    }

    if (deleteKeys.size) {
      const deleteRes = await client.query(
        `DELETE FROM "${tableName}" WHERE ${compositeExpr} = ANY($1)`,
        [Array.from(deleteKeys)]
      );
      deletedCount += deleteRes.rowCount;
    }

    if (dateColumn && minDate && maxDate && csvKeys.size) {
      const deleteExistingRes = await client.query(
        `DELETE FROM "${tableName}" WHERE "${dateColumn}" BETWEEN $1 AND $2 AND NOT (${compositeExpr} = ANY($3))`,
        [minDate, maxDate, Array.from(csvKeys)]
      );
      deletedCount += deleteExistingRes.rowCount;
    }

    await client.query('COMMIT');
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);

    return {
      processedRows,
      insertedOrUpdated,
      deletedCount,
      skippedRows: skippedRows.length,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    throw err;
  }
}

// ------------------ BILLED AR AGING IMPORT ------------------
async function importBilledArAging(filePath, tableName, options = {}) {
  const { onProgress } = options;
  const BATCH_SIZE = 500;
  let processedRows = 0, insertedOrUpdated = 0, deletedCount = 0;
  const skippedRows = [];
  const deleteKeys = new Set();
  const csvKeys = new Set();
  const keyColumns = ['Service Dates', 'Med Rec #', 'Invoice Num'];
  let minDate = null, maxDate = null;

  await ensureBilledArAgingConstraint();

  const result = await client.query(`
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1 AND is_generated = 'NEVER'
    ORDER BY ordinal_position
  `, [tableName]);

  const allColumns = result.rows.map(r => r.column_name);
  const tableColumns = allColumns.filter(c => !keyColumns.includes(c));
  const dateColumn = allColumns.find(c => /bill\s*date/i.test(c)) || allColumns.find(c => /date/i.test(c));

  await client.query('BEGIN');

  try {
    let batch = [];
    const parser = fs.createReadStream(filePath).pipe(parse(CSV_OPTS_TOLERANT));

    for await (const row of parser) {
      processedRows++;
      if (typeof onProgress === 'function') onProgress(processedRows);
      const keys = keyColumns.map(k => row[k]);
      const action = (row['Action'] || '').toUpperCase();
      const dateStr = dateColumn ? row[dateColumn] : null;
      if (dateStr) {
        const d = new Date(dateStr);
        if (!isNaN(d)) {
          if (!minDate || d < minDate) minDate = d;
          if (!maxDate || d > maxDate) maxDate = d;
        }
      }

      if (keys.some(v => !v)) {
        skippedRows.push({ row, reason: 'Missing key columns' });
        continue;
      }

      const composite = keys.join('|');
      csvKeys.add(composite);

      if (action === 'DELETE') {
        deleteKeys.add(composite);
        continue;
      }

      const rowData = tableColumns.map(c => toNullIfEmpty(row[c]));
      batch.push({ keys, rowData });
      if (batch.length >= BATCH_SIZE) {
        await processBatch(batch, tableName, tableColumns, keyColumns);
        insertedOrUpdated += batch.length;
        batch = [];
      }
    }

    if (batch.length) {
      await processBatch(batch, tableName, tableColumns, keyColumns);
      insertedOrUpdated += batch.length;
    }

    const compositeExpr =
      `COALESCE("Service Dates"::text,'') || '|' || COALESCE("Med Rec #"::text,'') || '|' || COALESCE("Invoice Num"::text,'')`;

    if (deleteKeys.size) {
      const deleteRes = await client.query(
        `DELETE FROM "${tableName}" WHERE ${compositeExpr} = ANY($1)`,
        [Array.from(deleteKeys)]
      );
      deletedCount += deleteRes.rowCount;
    }

    if (dateColumn && minDate && maxDate && csvKeys.size) {
      const deleteExistingRes = await client.query(
        `DELETE FROM "${tableName}" WHERE "${dateColumn}" BETWEEN $1 AND $2 AND NOT (${compositeExpr} = ANY($3))`,
        [minDate, maxDate, Array.from(csvKeys)]
      );
      deletedCount += deleteExistingRes.rowCount;
    }

    await client.query('COMMIT');
    fs.unlinkSync(filePath);

    return {
      processedRows,
      insertedOrUpdated,
      deletedCount,
      skippedRows: skippedRows.length,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    throw err;
  }
}

// ------------------ QUEUE MANAGER IMPORT ------------------
async function importQueueManager(filePath, tableName, options = {}) {
  const { onProgress } = options;
  const BATCH_SIZE = 500;
  let processedRows = 0, insertedOrUpdated = 0, deletedCount = 0;
  const skippedRows = [];
  const deleteIds = new Set();
  const csvIds = new Set();
  let minDate = null, maxDate = null;

  await ensureQueueManagerConstraint();

  const result = await client.query(
    `
    SELECT column_name, data_type, character_maximum_length
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = $1 AND is_generated = 'NEVER'
    ORDER BY ordinal_position
    `,
    [tableName]
  );

  const columnMeta = new Map(
    result.rows.map(r => [r.column_name, {
      dataType: r.data_type,
      maxLength: r.character_maximum_length,
    }])
  );

  const allColumns = result.rows.map(r => r.column_name);
  const keyColumn = 'QueueID';
  const tableColumns = allColumns.filter(c => c !== keyColumn);
  const dateColumn = allColumns.find(c => /date/i.test(c));

  const isDateCol = {};
  for (const [columnName, meta] of columnMeta.entries()) {
    const dataType = meta.dataType || '';
    isDateCol[columnName] = /date/i.test(dataType) || /date/i.test(columnName);
  }

  const keyColumnMeta = columnMeta.get(keyColumn);

  await client.query('BEGIN');

  try {
    let batch = [];
    const parser = fs.createReadStream(filePath).pipe(parse(CSV_OPTS_TOLERANT));

    for await (const row of parser) {
      processedRows++;
      if (typeof onProgress === 'function') onProgress(processedRows);
      const idRaw = row[keyColumn];
      const id = typeof idRaw === 'string' ? idRaw.trim() : idRaw;
      const action = (row['Action'] || '').toUpperCase();

      const dateStr = dateColumn ? (isDateCol[dateColumn] ? normalizeDateForPg(row[dateColumn]) : row[dateColumn]) : null;
      if (dateStr) {
        const d = new Date(dateStr);
        if (!isNaN(d)) {
          if (!minDate || d < minDate) minDate = d;
          if (!maxDate || d > maxDate) maxDate = d;
        }
      }

      if (!id) {
        skippedRows.push({ row, reason: 'Missing key column' });
        continue;
      }

      if (
        keyColumnMeta?.maxLength != null &&
        typeof id === 'string' &&
        id.length > keyColumnMeta.maxLength
      ) {
        skippedRows.push({
          row,
          reason: `Key column exceeds maximum length of ${keyColumnMeta.maxLength}`,
        });
        continue;
      }

      csvIds.add(id);

      if (action === 'DELETE') {
        deleteIds.add(id);
        continue;
      }

      const rowData = tableColumns.map(c => {
        let value = row[c];

        if (isDateCol[c]) {
          value = normalizeDateForPg(value);
        } else {
          value = toNullIfEmpty(value);
        }

        if (
          value !== null &&
          typeof value === 'string'
        ) {
          const maxLength = columnMeta.get(c)?.maxLength;
          if (typeof maxLength === 'number' && value.length > maxLength) {
            value = value.slice(0, maxLength);
          }
        }

        return value;
      });

      batch.push({ keys: [id], rowData });
      if (batch.length >= BATCH_SIZE) {
        await processBatch(batch, tableName, tableColumns, [keyColumn]);
        insertedOrUpdated += batch.length;
        batch = [];
      }
    }

    if (batch.length) {
      await processBatch(batch, tableName, tableColumns, [keyColumn]);
      insertedOrUpdated += batch.length;
    }

    if (deleteIds.size) {
      const deleteRes = await client.query(
        `DELETE FROM "${tableName}" WHERE "${keyColumn}" = ANY($1)`,
        [Array.from(deleteIds)]
      );
      deletedCount += deleteRes.rowCount;
    }

    if (dateColumn && minDate && maxDate && csvIds.size) {
      const deleteExistingRes = await client.query(
        `DELETE FROM "${tableName}" WHERE "${dateColumn}" BETWEEN $1 AND $2 AND NOT ("${keyColumn}" = ANY($3))`,
        [minDate, maxDate, Array.from(csvIds)]
      );
      deletedCount += deleteExistingRes.rowCount;
    }

    await client.query('COMMIT');
    fs.unlinkSync(filePath);

    return {
      processedRows,
      insertedOrUpdated,
      deletedCount,
      skippedRows: skippedRows.length,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    throw err;
  }
}

// ------------------ ROOT ------------------
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

if (process.env.NODE_ENV === 'test') {
  module.exports = {
    processBatch,
    setClientForTesting,
    normalizeDateForPg,
    importAdmissions,
    importAdmissionIdTable,
    importActiveAgency,
    importRawData,
    ensureAdmissionsConstraint,
    ensureActiveAgencyConstraint,
    ensureRawDataConstraint,
  };
} else {
  app.listen(PORT, () => console.log(`🚀 Unified server running at http://localhost:${PORT}`));
}
