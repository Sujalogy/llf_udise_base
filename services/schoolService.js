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
      throw { status: error.response.status, data: error.response.data };
    }
    throw {
      status: 500,
      message: "Error proxying request",
      error: error.message,
    };
  }
};

const getExistingCodes = async (codes, ay) => {
  if (!codes || codes.length === 0) return [];

  // Verify table exists
  const checkTable = await pool.query(
    format("SELECT to_regclass(%L)", TABLE_NAME)
  );
  if (!checkTable.rows[0].to_regclass) return [];

  // Check if 'ay' column exists
  const checkCol = await pool.query(
    format(
      "SELECT column_name FROM information_schema.columns WHERE table_name = %L AND column_name = 'ay'",
      TABLE_NAME
    )
  );

  let query;
  // If we have an AY string and the column exists, check the pair
  if (checkCol.rows.length > 0 && ay) {
    query = format(
      "SELECT udise_code FROM %I WHERE udise_code IN (%L) AND ay = %L",
      TABLE_NAME,
      codes,
      ay
    );
  } else {
    // Fallback if no AY provided
    query = format(
      "SELECT udise_code FROM %I WHERE udise_code IN (%L)",
      TABLE_NAME,
      codes
    );
  }

  const result = await pool.query(query);
  return result.rows.map((r) => r.udise_code);
};

// 2. UPDATE: saveSchoolsToDb to enforce composite uniqueness
const saveSchoolsToDb = async (schoolsData) => {
  if (!schoolsData || schoolsData.length === 0)
    return { success: false, count: 0 };

  const client = await pool.connect();
  try {
    const inputColumns = Object.keys(schoolsData[0]);
    await client.query("BEGIN");

    // Check/Create Table Logic
    const checkTableQuery = format("SELECT to_regclass(%L)", TABLE_NAME);
    const tableCheckResult = await client.query(checkTableQuery);

    if (!tableCheckResult.rows[0].to_regclass) {
      const columnDefinitions = inputColumns
        .map((col) => {
          // Remove UNIQUE from single column definition
          if (col === "udise_code") return "udise_code TEXT";
          return format("%I TEXT", col);
        })
        .join(", ");
      await client.query(
        format(
          "CREATE TABLE %I (local_id SERIAL PRIMARY KEY, %s)",
          TABLE_NAME,
          columnDefinitions
        )
      );
    } else {
      // Add Missing Columns
      const getColQuery = format(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %L",
        TABLE_NAME
      );
      const existingColsRes = await client.query(getColQuery);
      const existingCols = new Set(
        existingColsRes.rows.map((r) => r.column_name)
      );
      for (const col of inputColumns) {
        if (!existingCols.has(col)) {
          await client.query(
            format("ALTER TABLE %I ADD COLUMN %I TEXT", TABLE_NAME, col)
          );
        }
      }
    }

    // --- CRITICAL: COMPOSITE UNIQUE INDEX (UDISE_CODE + AY) ---
    // Drop old index if exists
    await client.query(`DROP INDEX IF EXISTS idx_udise_code_unique`);

    // Create new composite index
    if (inputColumns.includes("ay")) {
      await client.query(
        `CREATE UNIQUE INDEX IF NOT EXISTS idx_udise_ay_unique ON ${TABLE_NAME} (udise_code, ay)`
      );

      const values = schoolsData.map((school) =>
        inputColumns.map((col) => {
          let val = school[col];
          if (val === "" || val === undefined || val === null) return "NA";
          if (typeof val === "object") return JSON.stringify(val);
          return val;
        })
      );

      const query = format(
        "INSERT INTO %I (%I) VALUES %L ON CONFLICT (udise_code, ay) DO NOTHING RETURNING local_id",
        TABLE_NAME,
        inputColumns,
        values
      );
      const result = await client.query(query);
      await client.query("COMMIT");
      return { success: true, count: result.rowCount };
    }

    // Fallback (for old data structure)
    const values = schoolsData.map((s) =>
      inputColumns.map((c) =>
        typeof s[c] === "object" ? JSON.stringify(s[c]) : s[c] || "NA"
      )
    );
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

const getDashboardStats = async (filters = {}) => {
  const { state, district, block, ay } = filters;

  try {
    const checkTable = await pool.query(
      format("SELECT to_regclass(%L)", TABLE_NAME)
    );
    if (!checkTable.rows[0].to_regclass) return getEmptyStats();

    const conditions = [];
    const params = [];
    let paramCount = 1;

    if (state) {
      conditions.push(`state = $${paramCount++}`);
      params.push(state);
    }
    if (district) {
      conditions.push(`district = $${paramCount++}`);
      params.push(district);
    }
    if (block) {
      conditions.push(`block = $${paramCount++}`);
      params.push(block);
    }
    if (ay) {
      conditions.push(`ay = $${paramCount++}`);
      params.push(ay);
    }

    const whereClause =
      conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    const [
      totalRecordsResult,
      uniqueUdiseResult,
      uniqueStatesResult,
      uniqueDistrictsResult,
      uniqueBlocksResult,
      uniqueAYResult,
    ] = await Promise.all([
      pool.query(
        format("SELECT COUNT(*) as count FROM %I %s", TABLE_NAME, whereClause),
        params
      ),
      pool.query(
        format(
          "SELECT COUNT(DISTINCT udise_code) as count FROM %I %s",
          TABLE_NAME,
          whereClause
        ),
        params
      ),
      pool.query(
        format(
          "SELECT COUNT(DISTINCT state) as count FROM %I %s",
          TABLE_NAME,
          whereClause
        ),
        params
      ),
      pool.query(
        format(
          "SELECT COUNT(DISTINCT district) as count FROM %I %s",
          TABLE_NAME,
          whereClause
        ),
        params
      ),
      pool.query(
        format(
          "SELECT COUNT(DISTINCT block) as count FROM %I %s",
          TABLE_NAME,
          whereClause
        ),
        params
      ),
      pool.query(
        format(
          "SELECT COUNT(DISTINCT ay) as count FROM %I %s AND ay IS NOT NULL",
          TABLE_NAME,
          whereClause
        ),
        params
      ),
    ]);

    return {
      totalRecords: parseInt(totalRecordsResult.rows[0]?.count || 0),
      uniqueUdise: parseInt(uniqueUdiseResult.rows[0]?.count || 0),
      uniqueStates: parseInt(uniqueStatesResult.rows[0]?.count || 0),
      uniqueDistricts: parseInt(uniqueDistrictsResult.rows[0]?.count || 0),
      uniqueBlocks: parseInt(uniqueBlocksResult.rows[0]?.count || 0),
      uniqueAcademicYears: parseInt(uniqueAYResult.rows[0]?.count || 0),
      appliedFilters: { state, district, block, ay },
    };
  } catch (err) {
    console.error("Dashboard Stats Error:", err);
    throw err;
  }
};

const getAcademicYears = async () => {
  try {
    const result = await pool.query(
      format(
        "SELECT DISTINCT ay FROM %I WHERE ay IS NOT NULL AND ay != 'NA' ORDER BY ay DESC",
        TABLE_NAME
      )
    );
    return result.rows.map((r) => r.ay);
  } catch (err) {
    if (err.code === "42P01") return [];
    throw err;
  }
};

const getAllFilterOptions = async () => {
  try {
    const [states, districts, blocks, academicYears] = await Promise.all([
      pool.query(
        format(
          "SELECT DISTINCT state FROM %I WHERE state IS NOT NULL ORDER BY state",
          TABLE_NAME
        )
      ),
      pool.query(
        format(
          "SELECT DISTINCT state, district FROM %I WHERE state IS NOT NULL AND district IS NOT NULL ORDER BY state, district",
          TABLE_NAME
        )
      ),
      pool.query(
        format(
          "SELECT DISTINCT state, district, block FROM %I WHERE state IS NOT NULL AND district IS NOT NULL AND block IS NOT NULL ORDER BY state, district, block",
          TABLE_NAME
        )
      ),
      pool.query(
        format(
          "SELECT DISTINCT ay FROM %I WHERE ay IS NOT NULL AND ay != 'NA' ORDER BY ay DESC",
          TABLE_NAME
        )
      ),
    ]);

    const districtsByState = {};
    districts.rows.forEach((row) => {
      if (!districtsByState[row.state]) districtsByState[row.state] = [];
      if (!districtsByState[row.state].includes(row.district)) {
        districtsByState[row.state].push(row.district);
      }
    });

    const blocksByStateDistrict = {};
    blocks.rows.forEach((row) => {
      const key = `${row.state}|${row.district}`;
      if (!blocksByStateDistrict[key]) blocksByStateDistrict[key] = [];
      if (!blocksByStateDistrict[key].includes(row.block)) {
        blocksByStateDistrict[key].push(row.block);
      }
    });

    return {
      states: states.rows.map((r) => r.state),
      districtsByState,
      blocksByStateDistrict,
      academicYears: academicYears.rows.map((r) => r.ay),
    };
  } catch (err) {
    if (err.code === "42P01")
      return {
        states: [],
        districtsByState: {},
        blocksByStateDistrict: {},
        academicYears: [],
      };
    throw err;
  }
};

function getEmptyStats() {
  return {
    totalRecords: 0,
    uniqueUdise: 0,
    uniqueStates: 0,
    uniqueDistricts: 0,
    uniqueBlocks: 0,
    uniqueAcademicYears: 0,
    appliedFilters: {},
  };
}

// UPDATE module.exports at the end:
module.exports = {
  proxyUdiseRequest,
  saveSchoolsToDb,
  getFiltersFromDb,
  searchSchoolsInDb,
  getExistingCodes,
  getDashboardStats, // ADD
  getAcademicYears, // ADD
  getAllFilterOptions, // ADD
};
