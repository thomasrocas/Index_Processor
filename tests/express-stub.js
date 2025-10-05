function noopMiddleware() {
  return (req, res, next) => {
    if (typeof next === 'function') next();
  };
}

function createExpress() {
  return {
    use: () => {},
    post: () => {},
    get: () => {},
    listen: (port, cb) => {
      if (typeof cb === 'function') cb();
      return { close: () => {} };
    },
  };
}

createExpress.static = noopMiddleware;
createExpress.urlencoded = () => noopMiddleware();

module.exports = createExpress;
