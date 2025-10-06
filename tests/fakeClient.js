class FakeClient {
  constructor(options = {}) {
    if (typeof options === 'string') {
      this.tableName = options;
    } else {
      this.tableName = options.tableName || 'pdgm';
    }
    this.storage = new Map();
    this.lastConflictColumns = [];
  }

  async query(sql, params = []) {
    const tableName = this.tableName.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
    const insertRegex = new RegExp(`^\\s*INSERT\\s+INTO\\s+"${tableName}"`, 'i');
    const deleteRegex = new RegExp(`^\\s*DELETE\\s+FROM\\s+"${tableName}"`, 'i');

    if (insertRegex.test(sql)) {
      const insertMatch = sql.match(new RegExp(`INSERT\\s+INTO\\s+"${tableName}"\\s*\\(([^)]+)\\)`, 'i'));
      if (!insertMatch) {
        throw new Error('Unable to parse INSERT statement for columns.');
      }
      const columns = insertMatch[1]
        .split(',')
        .map(part => part.trim().replace(/"/g, ''));

      const conflictMatch = sql.match(/ON\s+CONFLICT\s*\(([^)]+)\)/i);
      if (!conflictMatch) {
        throw new Error('Unable to parse conflict columns.');
      }
      const conflictColumns = conflictMatch[1]
        .split(',')
        .map(part => part.trim().replace(/"/g, ''));

      this.lastConflictColumns = conflictColumns;

      const rowWidth = columns.length;
      if (params.length % rowWidth !== 0) {
        throw new Error('Parameter length does not align with columns.');
      }

      const rowCount = params.length / rowWidth;
      for (let i = 0; i < rowCount; i++) {
        const rowValues = params.slice(i * rowWidth, (i + 1) * rowWidth);
        const record = {};
        columns.forEach((col, idx) => {
          record[col] = rowValues[idx];
        });

        const key = conflictColumns.map(col => record[col]).join('|');
        this.storage.set(key, record);
      }

      return { rowCount };
    }

    if (deleteRegex.test(sql)) {
      const whereMatch = sql.match(/WHERE\s+"([^"\s]+)"\s*=\s*ANY\(\$1\)/i);
      if (!whereMatch) {
        throw new Error('Unable to parse DELETE statement for key column.');
      }

      const keyColumn = whereMatch[1];
      const ids = Array.isArray(params[0]) ? params[0] : [];
      const idSet = new Set(ids.map(v => (v === null || v === undefined ? v : String(v))));
      let deleted = 0;

      for (const [key, record] of Array.from(this.storage.entries())) {
        const value = record[keyColumn];
        const normalizedValue = value === null || value === undefined ? value : String(value);
        if (idSet.has(normalizedValue)) {
          this.storage.delete(key);
          deleted++;
        }
      }

      return { rowCount: deleted };
    }

    throw new Error(`FakeClient received unsupported query: ${sql}`);
  }

  getAllRows() {
    return Array.from(this.storage.values());
  }
}

module.exports = FakeClient;
