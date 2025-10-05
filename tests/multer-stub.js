function multer() {
  return {
    single: () => (req, res, next) => {
      if (typeof next === 'function') next();
    },
    any: () => (req, res, next) => {
      if (typeof next === 'function') next();
    },
  };
}

module.exports = multer;
