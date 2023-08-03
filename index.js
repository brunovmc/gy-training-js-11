const express = require('express');
const fs = require('fs');
const csvParser = require('csv-parser');
const path = require('path');

const app = express();
const port = 3000;

function parseDateTime(dateTimeString) {
  const [datePart, timePart] = dateTimeString.split(' ');

  const [day, month, year] = datePart.split('/');
  const [hour, minute] = timePart.split(':');

  return new Date(year, month - 1, day, hour, minute);
}

function calculateWeeklyAggregation(file) {
  const weeklyAggregation = {};
  const alarmThreshold = 1;

  fs.createReadStream(file)
    .pipe(csvParser({ separator: ';' }))
    .on('data', (row) => {
      const date = parseDateTime(row.dateTime);
      date.setHours(0, 0, 0, 0);

      const firstDayOfWeek = new Date(date);
      firstDayOfWeek.setDate(date.getDate() - date.getDay());

      const isoDateString = firstDayOfWeek.toISOString().split('T')[0];

      const value = parseFloat(row.value);

      if (!isNaN(value)) {
        if (weeklyAggregation[isoDateString]) {
          weeklyAggregation[isoDateString].sum += value;
          weeklyAggregation[isoDateString].count++;

          if (value > alarmThreshold) {
            weeklyAggregation[isoDateString].alarmCount++;
          }
        } else {
          weeklyAggregation[isoDateString] = { sum: value, count: 1, alarmCount: value > alarmThreshold ? 1 : 0 };
        }
      }
    })
    .on('end', () => {
      console.log("Weekly Aggregation Report:");
      for (const date in weeklyAggregation) {
        weeklyAggregation[date].average = weeklyAggregation[date].sum / weeklyAggregation[date].count;
        console.log(`${date} - Weekly Average: ${weeklyAggregation[date].average.toFixed(2)} - Alarms Triggered: ${weeklyAggregation[date].alarmCount}`);
      }
    });
}

app.get('/generate-report', (req, res) => {
  const file = path.join(__dirname, 'METRICS_REPORT-1673351714089 (2).csv');
  calculateWeeklyAggregation(file);

  res.send("Weekly Aggregation Report generated and logged in the console.");
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
