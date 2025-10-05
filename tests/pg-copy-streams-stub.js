module.exports = {
  from: () => {
    throw new Error('pg-copy-streams stub should not be invoked during tests.');
  },
};
