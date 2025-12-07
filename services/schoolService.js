const pool = require("../db");
const format = require("pg-format");
const axios = require("axios");

const TABLE_NAME = "udise_data";

// --- Service: Fetch from External UDISE API ---
const proxyUdiseRequest = async (method, url, data) => {
  const targetBaseUrl = "https://kys.udiseplus.gov.in/webapp/api";
  const targetUrl = `${targetBaseUrl}${url}`;

  console.log(`[Proxy Service] Forwarding to: ${targetUrl}`);

  try {
    const response = await axios({
      method: method,
      url: targetUrl,
      data: method === "POST" || method === "PUT" ? data : undefined,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        Accept: "application/json, text/plain, */*",
        Origin: "https://kys.udiseplus.gov.in",
        Referer: "https://kys.udiseplus.gov.in/",
      },
    });
    return response.data;
  } catch (error) {
    if (error.response) {
      console.error(
        `[Proxy Error] Remote API returned ${error.response.status}`
      );
      throw { status: error.response.status, data: error.response.data };
    }
    throw {
      status: 500,
      message: "Error proxying request",
      error: error.message,
    };
  }
};

// --- Service: Save Data to DB ---
const saveSchoolsToDb = async (schoolsData) => {
  const client = await pool.connect();
  try {
    const columns = Object.keys(schoolsData[0]);

    // Check/Create Table
    const checkTableQuery = format("SELECT to_regclass(%L)", TABLE_NAME);
    const tableCheckResult = await client.query(checkTableQuery);

    if (!tableCheckResult.rows[0].to_regclass) {
      const columnDefinitions = columns
        .map((col) => format("%I TEXT", col))
        .join(", ");
      const createTableQuery = format(
        "CREATE TABLE %I (local_id SERIAL PRIMARY KEY, %s)",
        TABLE_NAME,
        columnDefinitions
      );
      console.log(createTableQuery)
      await client.query(createTableQuery);
    }

    // Insert Data
    const values = schoolsData.map((school) => {
      return columns.map((col) => {
        const val = school[col];
        return val === "" || val === undefined ? null : val;
      });
    });

    const query = format(
      "INSERT INTO %I (%I) VALUES %L RETURNING local_id",
      TABLE_NAME,
      columns,
      values
    );

    await client.query("BEGIN");
    const result = await client.query(query);
    await client.query("COMMIT");

    return { success: true, count: result.rowCount };
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
};

// --- Service: Get Filters ---
const getFiltersFromDb = async () => {
  try {
    const query = format(
      `SELECT DISTINCT state, district FROM %I WHERE state IS NOT NULL ORDER BY state, district`,
      TABLE_NAME
    );
    const result = await pool.query(query);

    const hierarchy = {};
    result.rows.forEach((row) => {
      if (!hierarchy[row.state]) hierarchy[row.state] = [];
      if (!hierarchy[row.state].includes(row.district))
        hierarchy[row.state].push(row.district);
    });
    return hierarchy;
  } catch (err) {
    if (err.code === "42P01") return {};
    throw err;
  }
};

// --- Service: Search Data ---
const searchSchoolsInDb = async (state, districts) => {
  try {
    let queryText = format("SELECT * FROM %I WHERE 1=1", TABLE_NAME);
    const queryValues = [];
    let paramCounter = 1;

    if (state) {
      queryText += ` AND state = $${paramCounter++}`;
      queryValues.push(state);
    }
    if (districts && districts.length > 0) {
      queryText += ` AND district = ANY($${paramCounter++})`;
      queryValues.push(districts);
    }
    queryText += " LIMIT 100";
    const result = await pool.query(queryText, queryValues);
    return result.rows;
  } catch (err) {
    if (err.code === "42P01") return [];
    throw err;
  }
};

module.exports = {
  proxyUdiseRequest,
  saveSchoolsToDb,
  getFiltersFromDb,
  searchSchoolsInDb,
};
