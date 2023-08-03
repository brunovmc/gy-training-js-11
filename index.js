const express = require('express');
const fs = require('fs');
const csvParser = require('csv-parser');

const app = express();
const port = 3000;

function parseDateTime(dateTimeString) {
  const [datePart, timePart] = dateTimeString.split(' ');

  const [day, month, year] = datePart.split('/');
  const [hour, minute] = timePart.split(':');

  return new Date(year, month - 1, day, hour, minute);
}

function calculateWeeklyAggregation(file) {
  const weeklyData = {};
  const alarmThreshold = 1;
  let currentWeekStart = new Date('2023-01-06T08:45:00');

  fs.createReadStream(file)
    .pipe(csvParser({ separator: ';' }))
    .on('data', (row) => {
      const date = parseDateTime(row.dateTime);

      if (date >= currentWeekStart && date < new Date(currentWeekStart.getTime() + 7 * 24 * 60 * 60 * 1000)) {
        const isoDateString = currentWeekStart.toISOString().split('T')[0];
        const value = parseFloat(row.value);

        if (!isNaN(value)) {
          if (weeklyData[isoDateString]) {
            weeklyData[isoDateString].sum += value;
            weeklyData[isoDateString].count++;

            if (value > alarmThreshold) {
              weeklyData[isoDateString].alarmCount++;
            }
          } else {
            weeklyData[isoDateString] = { sum: value, count: 1, alarmCount: value > alarmThreshold ? 1 : 0 };
          }
        }
      } else if (date >= new Date(currentWeekStart.getTime() + 7 * 24 * 60 * 60 * 1000)) {
        currentWeekStart = new Date(currentWeekStart.getTime() + 7 * 24 * 60 * 60 * 1000);
      }
    })
    .on('end', () => {
      console.log("Weekly Aggregation Report:");
      for (const weekStartDate in weeklyData) {
        const { sum, count, alarmCount } = weeklyData[weekStartDate];
        const weeklyAverage = sum / count;
        console.log(`${weekStartDate} - Weekly Average: ${weeklyAverage.toFixed(2)} - Alarms Triggered: ${alarmCount}`);
      }
    });
}

app.get('/generate-report', (req, res) => {
  const file = 'METRICS_REPORT-1673351714089 (2).csv';
  calculateWeeklyAggregation(file);

  res.send("Weekly Aggregation Report generated and logged in the console.");
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
