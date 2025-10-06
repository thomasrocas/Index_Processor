const { Transform } = require('stream');

module.exports = {
  parse: () => {
    let buffer = '';
    return new Transform({
      readableObjectMode: true,
      transform(chunk, _enc, callback) {
        buffer += chunk.toString('utf8');
        callback();
      },
      flush(callback) {
        const lines = buffer
          .split(/\r?\n/)
          .map(line => line.trim())
          .filter(Boolean);

        if (!lines.length) {
          callback();
          return;
        }

        const headers = lines[0].split(',').map(h => h.trim());
        for (let i = 1; i < lines.length; i++) {
          const rawLine = lines[i];
          if (!rawLine) continue;
          const cells = rawLine.split(',');
          const row = {};
          headers.forEach((header, idx) => {
            row[header] = (cells[idx] ?? '').trim();
          });
          this.push(row);
        }

        callback();
      },
    });
  },
};
