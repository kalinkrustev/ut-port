const hrtime = require('browser-process-hrtime');
const diff = (time, newtime) => (newtime[0] - time[0]) * 1000 + (newtime[1] - time[1]) / 1000000;
const now = () => hrtime();

module.exports = {
    diff,
    after: milliseconds => {
        let seconds = Math.round(milliseconds / 1000);
        let result = now();
        result[1] += Math.round((milliseconds - seconds * 1000) * 1000000);
        result[0] += seconds;
        if (result[1] >= 1000000000) {
            result[0]++;
            result[1] -= 1000000000;
        }
        return result;
    },
    isAfter: (time, timeout) => Array.isArray(timeout) && ((time[0] > timeout[0]) || (time[0] === timeout[0] && time[1] > timeout[1])),
    now
};
