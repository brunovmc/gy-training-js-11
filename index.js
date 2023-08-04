const express = require('express');
const fs = require('fs');
const csvParser = require('csv-parser');


const app = express();
const port = 3000;

async function calculateWeeklyAggregation(file) {
  const weeklyData = {};
  const alarmThreshold = 1;
  let currentWeekStart = null;

  const promise = () => new Promise((resolve, reject) => {
    fs.createReadStream(file)
      .pipe(csvParser({ separator: ';' }))
      .on('data', (row) => {
        const date = parseDateTime(row.dateTime);
  
        if (currentWeekStart === null) {
          currentWeekStart = new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0);
        }
  
        const nextWeekStart = new Date(currentWeekStart.getTime() + 7 * 24 * 60 * 60 * 1000);
        
        if (date >= nextWeekStart) {
          currentWeekStart = new Date(date.getFullYear(), date.getMonth(), date.getDate(), 0, 0, 0);
        }
  
        const isoDateString = currentWeekStart.toISOString().split('T')[0];
        const value = Number(row.value.replace(',', '.'));
  
        if (!isNaN(value)) {
          if (!weeklyData[isoDateString]) {
            weeklyData[isoDateString] = { sum: 0, count: 0, alarmCount: 0 };
          }
  
          weeklyData[isoDateString].sum += value;
          weeklyData[isoDateString].count++;
  
          if (value > alarmThreshold) {
            weeklyData[isoDateString].alarmCount++;
          }
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
    });
    return await promise();
}
function parseDateTime(dateTimeString) {
  const [datePart, timePart] = dateTimeString.split(' ');

  const [day, month, year] = datePart.split('/');
  const [hour, minute] = timePart.split(':');
  
  return new Date(year, month - 1, day, hour, minute);
}

app.get('/generate-report', async (req, res) => {
  const file = 'METRICS_REPORT-1673351714089 (2).csv';
  calculateWeeklyAggregation(file);
  res.send("Weekly Aggregation Report generated and logged in the console.");
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
