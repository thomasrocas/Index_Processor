class FakeClient {
  constructor(options = {}) {
    if (typeof options === 'string') {
      this.tableName = options;
      options = {};
    } else {
      this.tableName = options.tableName || 'pdgm';
    }

    this.storage = new Map();
    this.lastConflictColumns = [];
    this.columns = options.columns || Object.keys(options.columnTypes || {});
    this.columnTypes = options.columnTypes || {};
  }

  async query(sql, params = []) {
    const normalizedSql = sql.trim();
    const tableName = this.tableName.replace(/[-/\\^$*+?.()|[\]{}]/g, '\\$&');
    const insertRegex = new RegExp(`^INSERT\\s+INTO\\s+"${tableName}"`, 'i');
    const deleteRegex = new RegExp(`^DELETE\\s+FROM\\s+"${tableName}"`, 'i');

    if (/^(BEGIN|COMMIT|ROLLBACK)\b/i.test(normalizedSql)) {
      return { rowCount: 0 };
    }

    if (/FROM\s+information_schema\.columns/i.test(normalizedSql)) {
      if (/is_generated\s*=\s*'NEVER'/i.test(normalizedSql)) {
        return {
          rows: this.columns.map(column_name => ({ column_name })),
        };
      }

      if (/data_type/i.test(normalizedSql)) {
        return {
          rows: Object.entries(this.columnTypes).map(([column_name, data_type]) => ({
            column_name,
            data_type,
          })),
        };
      }
    }

    if (insertRegex.test(normalizedSql)) {
      const insertMatch = normalizedSql.match(
        new RegExp(`INSERT\\s+INTO\\s+"${tableName}"\\s*\\(([^)]+)\\)`, 'i')
      );
      if (!insertMatch) {
        throw new Error('Unable to parse INSERT statement for columns.');
      }
      const columns = insertMatch[1]
        .split(',')
        .map(part => part.trim().replace(/"/g, ''));

      const conflictMatch = normalizedSql.match(/ON\s+CONFLICT\s*\(([^)]+)\)/i);
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

    if (deleteRegex.test(normalizedSql)) {
      if (/BETWEEN\s+\$1\s+AND\s+\$2/i.test(normalizedSql)) {
        return { rowCount: 0 };
      }

      const whereMatch = normalizedSql.match(/WHERE\s+"([^"]+)"\s*=\s*ANY\(\$(\d+)\)/i);
      if (!whereMatch) {
        return { rowCount: 0 };
      }

      const keyColumn = whereMatch[1];
      const paramIndex = parseInt(whereMatch[2], 10) - 1;
      const ids = Array.isArray(params[paramIndex]) ? params[paramIndex] : [];
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
