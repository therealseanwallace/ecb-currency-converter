import fs from "fs";
import path from "path";
import csv from "csv-parser";
import { createObjectCsvWriter as createCsvWriter } from "csv-writer";
import axios from "axios";

const delayPeriod = 2000;

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Define directories
const workingDir = path.join(path.resolve(), "/working");

// Function to check if a CSV file has the required columns
const hasRequiredColumns = (headers) => {
  const requiredColumns = [
    "Date",
    "Amount",
    "Currency",
    "Output currency",
    "Daily rate",
    "Output",
  ];
  return requiredColumns.every((col) => headers.includes(col));
};

const adjustDateByOneDayBack = (dateString) => {
  const date = new Date(dateString);
  date.setDate(date.getDate() - 1);
  return date.toISOString().split("T")[0]; // Returns the adjusted date in YYYY-MM-DD format
};

// Function to fetch exchange rate from ECB API
const fetchExchangeRate = async (date, baseCurrency, targetCurrency) => {
  // Set a timeout so we don't smash the API
  await delay(delayPeriod);
  let dateToSearch = date;
  const url = `https://data-api.ecb.europa.eu/service/data/EXR/D.${baseCurrency}.${targetCurrency}.SP00.A`;
  const params = {
    startPeriod: dateToSearch,
    endPeriod: dateToSearch,
    detail: "dataonly",
  };

  try {
    console.log(
      `Fetching exchange rate for ${baseCurrency} to ${targetCurrency}`
    );

    let dataFound = false;
    let attempts = 0;
    const maxAttempts = 10; // Prevent infinite loops

    while (!dataFound && attempts < maxAttempts) {
      console.log(`Trying for date: ${dateToSearch}`);
      const response = await axios.get(url, { params });

      if (response.status === 200 && response.data) {
        dataFound = true;
        if (
          response.data &&
          response.data.dataSets &&
          response.data.dataSets[0].series
        ) {
          const seriesKey = Object.keys(response.data.dataSets[0].series)[0];
          const rate =
            response.data.dataSets[0].series[seriesKey].observations["0"][0];
          return rate;
        }
        return null;
      } else {
        console.log(
          "No data found for the given date. Trying for the previous date."
        );
        dateToSearch = adjustDateByOneDayBack(dateToSearch); // Adjust the date by one day back, implement this function
        params.startPeriod = dateToSearch;
        params.endPeriod = dateToSearch;
        await delay(delayPeriod); // Wait for 1 second before the next attempt to avoid spamming the API
      }

      attempts++;
    }

    if (!dataFound) {
      console.log("Failed to fetch data after maximum attempts.");
    }
    console.log(
      `Fetching exchange rate for ${baseCurrency} to ${targetCurrency} on ${date}`
    );
  } catch (error) {
    console.error(`Error fetching exchange rate: ${error}`);
    return null;
  }
};

// Function to process CSV files
const processCsvFile = async (filePath) => {
  const results = [];
  const headers = [];

  fs.createReadStream(filePath)
    .pipe(csv())
    .on("headers", (headerList) => {
      headers.push(...headerList);
      if (!hasRequiredColumns(headers)) {
        console.error(`File ${filePath} does not have the required columns.`);
        process.exit(1);
      }
    })
    .on("data", (data) => results.push(data))
    .on("end", async () => {
      for (const row of results) {
        const rate = await fetchExchangeRate(
          row.Date,
          row.Currency,
          row["Output currency"]
        );

        if (rate) {
          row["Daily rate"] = rate;
          const invertedRate = 1 / rate;
          console.log("Rate found:", invertedRate);
          row["Output"] = (parseFloat(row.Amount) * invertedRate).toFixed(2);
        } else {
          console.error(
            `No rate found for ${row.Currency} to ${row["Output currency"]} on ${row.Date}`
          );
        }
      }

      // Write updated data to a new CSV file
      const outputFilePath = path.join(
        path.dirname(filePath),
        `updated_${path.basename(filePath)}`
      );
      const csvWriter = createCsvWriter({
        path: outputFilePath,
        header: headers.map((header) => ({ id: header, title: header })),
      });

      await csvWriter.writeRecords(results);
      console.log(`File processed and saved as ${outputFilePath}`);
    });
};

// Function to process all CSV files in the working directory
fs.readdir(workingDir, (err, files) => {
  if (err) {
    console.error(`Error reading directory: ${err}`);
    process.exit(1);
  }

  files
    .filter((file) => file.endsWith(".csv"))
    .forEach((file) => {
      const filePath = path.join(workingDir, file);
      processCsvFile(filePath);
    });
});
