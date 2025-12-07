// ============================================================================
// --- FILE: services/schoolService.js ---
// ============================================================================
const pool = require("../db");
const format = require("pg-format");
const axios = require("axios");

const TABLE_NAME = "udise_data";

// --- 1. DEFINE CORE COLUMNS ---
// These are the columns you frequently query/filter by. 
// Everything else will be stored in the 'details' JSONB column.
const CORE_COLUMNS = [
  "udise_code", 
  "school_name", 
  "school_id", 
  "state", 
  "district", 
  "block", 
  "cluster", 
  "village", 
  "school_management", 
  "school_category", 
  "school_type", 
  "school_location",
  "is_operational", 
  "ay", 
  "lati", 
  "long", 
  "total_students",
  "local_id" // Auto-generated
];

// --- Service: Save Data to DB (Hybrid JSONB Approach) ---
const saveSchoolsToDb = async (schoolsData) => {
  if (!schoolsData || schoolsData.length === 0) return { success: false, count: 0 };

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // 1. Check/Create Table
    const checkTableQuery = format("SELECT to_regclass(%L)", TABLE_NAME);
    const tableCheckResult = await client.query(checkTableQuery);

    if (!tableCheckResult.rows[0].to_regclass) {
      console.log(`Creating table ${TABLE_NAME} with Hybrid JSONB Schema...`);
      
      // Define Core Columns explicitly
      const columnDefs = CORE_COLUMNS
        .filter(c => c !== 'local_id') // serial handled separately
        .map(col => format("%I TEXT", col));
      
      // Add the Magic 'details' column for all dynamic data
      columnDefs.push("details JSONB");

      const createTableQuery = format(
        "CREATE TABLE %I (local_id SERIAL PRIMARY KEY, %s)",
        TABLE_NAME,
        columnDefs.join(", ")
      );
      
      await client.query(createTableQuery);
    }

    // 2. Prepare Data for Insertion
    // We separate incoming data into 'Core' columns and 'Details' object
    const rowsToInsert = schoolsData.map(school => {
      const coreData = {};
      const extraData = {};

      Object.keys(school).forEach(key => {
        // Lowercase comparison to be safe
        if (CORE_COLUMNS.includes(key.toLowerCase()) && key !== 'local_id') {
          coreData[key] = school[key];
        } else {
          // If it's not a core column (like caste_general_total_boy), it goes to JSON
          extraData[key] = school[key];
        }
      });

      // Prepare row values array matching the insert order
      const rowValues = CORE_COLUMNS
        .filter(c => c !== 'local_id')
        .map(col => coreData[col] || null);
      
      // Add the JSON string as the last value
      rowValues.push(JSON.stringify(extraData));
      
      return rowValues;
    });

    // 3. Batch Insert
    // Columns to insert: Core Columns (minus ID) + 'details'
    const insertCols = [...CORE_COLUMNS.filter(c => c !== 'local_id'), 'details'];
    
    const insertQuery = format(
      "INSERT INTO %I (%I) VALUES %L",
      TABLE_NAME,
      insertCols,
      rowsToInsert
    );

    // Using basic query for mass insert
    const result = await client.query(insertQuery);
    await client.query("COMMIT");

    return { success: true, count: result.rowCount };

  } catch (err) {
    await client.query("ROLLBACK");
    console.error("Save Error:", err.message);
    
    // Suggest fix if table schema is wrong
    if (err.message.includes("column") && err.message.includes("does not exist")) {
      console.error("TIP: Your existing table might be using the old schema. Please DROP TABLE udise_data; and try again.");
    }
    
    throw err;
  } finally {
    client.release();
  }
};

// --- Service: Search Data (Auto-Flattening) ---
// This ensures your Frontend DataTable still receives a flat object
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
    
    queryText += " ORDER BY local_id DESC LIMIT 100";
    
    const result = await pool.query(queryText, queryValues);
    
    // FLATTEN THE DATA before sending to frontend
    // The frontend expects { school_name: "...", caste_total_boy: 10 }
    // The DB returns { school_name: "...", details: { caste_total_boy: 10 } }
    const flatRows = result.rows.map(row => {
      const { details, ...coreFields } = row;
      return { ...coreFields, ...(details || {}) };
    });

    return flatRows;
  } catch (err) {
    console.error("Search Error:", err.message);
    if (err.code === "42P01") return []; // Table doesn't exist
    throw err;
  }
};

// --- Service: Get Filters ---
const getFiltersFromDb = async () => {
  try {
    // Only core columns need to be queried for filters
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

// --- Service: Fetch from External UDISE API (Unchanged) ---
const proxyUdiseRequest = async (method, url, data) => {
  const targetBaseUrl = "https://kys.udiseplus.gov.in/webapp/api";
  const targetUrl = `${targetBaseUrl}${url}`;
  try {
    const response = await axios({
      method: method,
      url: targetUrl,
      data: method === "POST" || method === "PUT" ? data : undefined,
      headers: {
        "User-Agent": "Mozilla/5.0",
        Accept: "application/json, text/plain, */*",
      },
    });
    return response.data;
  } catch (error) {
    throw { status: error.response?.status || 500, message: error.message };
  }
};

module.exports = {
  proxyUdiseRequest,
  saveSchoolsToDb,
  getFiltersFromDb,
  searchSchoolsInDb,
};