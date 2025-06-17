const TRY_COUNT_MAX = 4;
const TIMEOUT_5_SEC = 5000;

const retry = async (operation, options = {}) => {
  const {maxAttempts = TRY_COUNT_MAX, baseDelay = TIMEOUT_5_SEC, onRetry = null} = options;

  let result = {};
  let tryCount = 0;
  while (tryCount <= maxAttempts) {
    try {
      result = await operation();
      break;
    } catch (error) {
      tryCount++;

      if (onRetry) {
        await onRetry(error);
      }

      await new Promise(resolve => setTimeout(resolve, baseDelay * Math.pow(2, tryCount)));
    }
  }
  return result;
};

module.exports = {retry};
