// ============================================================================
// --- FILE: services/schoolService.js ---
// ============================================================================
const pool = require("../db");
const format = require("pg-format");
const axios = require("axios");

const TABLE_NAME = "udise_data";

// --- Service: Fetch from External UDISE API ---
const proxyUdiseRequest = async (method, url, data) => {
  const targetBaseUrl = "https://kys.udiseplus.gov.in/webapp/api";
  const targetUrl = `${targetBaseUrl}${url}`;

  try {
    const response = await axios({
      method: method,
      url: targetUrl,
      data: method === "POST" || method === "PUT" ? data : undefined,
      headers: {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        Accept: "application/json, text/plain, */*",
        Origin: "https://kys.udiseplus.gov.in",
        Referer: "https://kys.udiseplus.gov.in/",
      },
    });
    return response.data;
  } catch (error) {
    if (error.response) {
      throw { status: error.response.status, data: error.response.data };
    }
    throw { status: 500, message: "Error proxying request", error: error.message };
  }
};

// --- Service: Get Existing Codes ---
const getExistingCodes = async (codes) => {
  if (!codes || codes.length === 0) return [];
  
  // Check if table exists first
  const checkTable = await pool.query(format("SELECT to_regclass(%L)", TABLE_NAME));
  if (!checkTable.rows[0].to_regclass) return [];

  const query = format("SELECT udise_code FROM %I WHERE udise_code IN (%L)", TABLE_NAME, codes);
  const result = await pool.query(query);
  return result.rows.map(r => r.udise_code);
};

// --- Service: Save Data to DB (Fixed Schema & Constraints) ---
const saveSchoolsToDb = async (schoolsData) => {
  if (!schoolsData || schoolsData.length === 0) return { success: false, count: 0 };

  const client = await pool.connect();
  try {
    // 1. Identify all columns from the incoming data
    const inputColumns = Object.keys(schoolsData[0]);

    await client.query("BEGIN");

    // 2. Check if table exists
    const checkTableQuery = format("SELECT to_regclass(%L)", TABLE_NAME);
    const tableCheckResult = await client.query(checkTableQuery);

    if (!tableCheckResult.rows[0].to_regclass) {
      // CASE A: Table does not exist -> Create it
      const columnDefinitions = inputColumns
        .map((col) => {
            if(col === 'udise_code') return "udise_code TEXT UNIQUE";
            return format("%I TEXT", col);
        })
        .join(", ");
      
      const createTableQuery = format(
        "CREATE TABLE %I (local_id SERIAL PRIMARY KEY, %s)",
        TABLE_NAME,
        columnDefinitions
      );
      await client.query(createTableQuery);
    } else {
      // CASE B: Table exists -> Check for missing columns (Schema Migration)
      const getColQuery = format(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %L",
        TABLE_NAME
      );
      const existingColsRes = await client.query(getColQuery);
      const existingCols = new Set(existingColsRes.rows.map(r => r.column_name));

      // Find which columns are new
      const missingCols = inputColumns.filter(col => !existingCols.has(col));

      if (missingCols.length > 0) {
        for (const col of missingCols) {
           // Add missing column dynamically
           const alterQuery = format("ALTER TABLE %I ADD COLUMN %I TEXT", TABLE_NAME, col);
           await client.query(alterQuery);
        }
      }

      // 3. CONSTRAINT CHECK (Simplified & Robust)
      // We ensure a UNIQUE INDEX exists on udise_code. This satisfies ON CONFLICT.
      await client.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_udise_code_unique ON ${TABLE_NAME} (udise_code)`);
    }

    // 4. Prepare Values (Handle "NA" & Objects)
    const values = schoolsData.map((school) => {
      return inputColumns.map((col) => {
        let val = school[col];
        // Handle Missing Values -> 'NA'
        if (val === "" || val === undefined || val === null) return 'NA';
        // Handle Objects -> JSON String (Prevents DB crash if data isn't flat)
        if (typeof val === 'object') return JSON.stringify(val);
        return val;
      });
    });

    // 5. Insert Data
    const query = format(
      "INSERT INTO %I (%I) VALUES %L ON CONFLICT (udise_code) DO NOTHING RETURNING local_id",
      TABLE_NAME,
      inputColumns,
      values
    );

    const result = await client.query(query);
    await client.query("COMMIT");

    return { success: true, count: result.rowCount };
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("DB Save Error:", err.message);
    throw err;
  } finally {
    client.release();
  }
};

// --- Service: Get Filters ---
const getFiltersFromDb = async () => {
  try {
    const query = format(
      `SELECT DISTINCT state, district FROM %I WHERE state IS NOT NULL AND state != 'NA' ORDER BY state, district`,
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
    if (err.code === "42P01") return {}; // Table doesn't exist yet
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
  getExistingCodes
};