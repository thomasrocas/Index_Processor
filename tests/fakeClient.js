class FakeClient {
  constructor() {
    this.storage = new Map();
  }

  async query(sql, params = []) {
    if (!/^\s*INSERT\s+INTO\s+"pdgm"/i.test(sql)) {
      throw new Error(`FakeClient received unsupported query: ${sql}`);
    }

    const insertMatch = sql.match(/INSERT\s+INTO\s+"pdgm"\s*\(([^)]+)\)/i);
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

  getAllRows() {
    return Array.from(this.storage.values());
  }
}

module.exports = FakeClient;
