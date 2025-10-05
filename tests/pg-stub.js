class StubClient {
  constructor() {}
  connect() {
    return Promise.resolve();
  }
  query() {
    return Promise.resolve({ rows: [], rowCount: 0 });
  }
  end() {
    return Promise.resolve();
  }
}

module.exports = { Client: StubClient };
