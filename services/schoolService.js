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

const getDashboardStats = async () => {
  const TABLE_NAME = "udise_data";
  
  try {
    // Check if table exists
    const checkTable = await pool.query(
      format("SELECT to_regclass(%L)", TABLE_NAME)
    );
    
    if (!checkTable.rows[0].to_regclass) {
      // Return empty stats if table doesn't exist
      return {
        totalRecords: 0,
        uniqueUdise: 0,
        uniqueStates: 0,
        uniqueDistricts: 0,
        uniqueBlocks: 0,
        uniqueClusters: 0,
        uniqueVillages: 0,
        totalStudents: 0,
        totalBoyStudents: 0,
        totalGirlStudents: 0,
        topStates: [],
        topDistricts: [],
        topBlocks: [],
        schoolsByCategory: [],
        schoolsByManagement: [],
      };
    }

    // First, dynamically check what columns exist in the table
    const columnsQuery = await pool.query(
      `SELECT column_name 
       FROM information_schema.columns 
       WHERE table_name = $1`,
      [TABLE_NAME]
    );
    
    const existingColumns = columnsQuery.rows.map(r => r.column_name);
    
    // Helper function to find the actual column name (case-insensitive)
    const findColumn = (possibleNames) => {
      for (const name of possibleNames) {
        const found = existingColumns.find(
          col => col.toLowerCase() === name.toLowerCase()
        );
        if (found) return found;
      }
      return null;
    };

    // Identify correct column names
    const totalStudentsCol = findColumn(['totalStudents', 'total_students', 'totalstudents']);
    const boyStudentsCol = findColumn(['totalBoyStudents', 'total_boy', 'totalboystudents']);
    const girlStudentsCol = findColumn(['totalGirlStudents', 'total_girl', 'totalgirlstudents']);

    // Build the student sum query dynamically
    let studentSumQuery = "SELECT 0 as total, 0 as boys, 0 as girls";
    
    if (totalStudentsCol || boyStudentsCol || girlStudentsCol) {
      const parts = [];
      
      if (totalStudentsCol) {
        parts.push(`
          COALESCE(
            SUM(
              CASE 
                WHEN "${totalStudentsCol}" IS NOT NULL 
                  AND "${totalStudentsCol}" != 'NA' 
                  AND "${totalStudentsCol}" != '' 
                THEN 
                  CASE 
                    WHEN "${totalStudentsCol}" ~ '^[0-9]+$' 
                    THEN CAST("${totalStudentsCol}" AS NUMERIC)
                    ELSE 0
                  END
                ELSE 0
              END
            ), 
            0
          ) as total
        `);
      } else {
        parts.push("0 as total");
      }

      if (boyStudentsCol) {
        parts.push(`
          COALESCE(
            SUM(
              CASE 
                WHEN "${boyStudentsCol}" IS NOT NULL 
                  AND "${boyStudentsCol}" != 'NA' 
                  AND "${boyStudentsCol}" != '' 
                THEN 
                  CASE 
                    WHEN "${boyStudentsCol}" ~ '^[0-9]+$' 
                    THEN CAST("${boyStudentsCol}" AS NUMERIC)
                    ELSE 0
                  END
                ELSE 0
              END
            ), 
            0
          ) as boys
        `);
      } else {
        parts.push("0 as boys");
      }

      if (girlStudentsCol) {
        parts.push(`
          COALESCE(
            SUM(
              CASE 
                WHEN "${girlStudentsCol}" IS NOT NULL 
                  AND "${girlStudentsCol}" != 'NA' 
                  AND "${girlStudentsCol}" != '' 
                THEN 
                  CASE 
                    WHEN "${girlStudentsCol}" ~ '^[0-9]+$' 
                    THEN CAST("${girlStudentsCol}" AS NUMERIC)
                    ELSE 0
                  END
                ELSE 0
              END
            ), 
            0
          ) as girls
        `);
      } else {
        parts.push("0 as girls");
      }

      studentSumQuery = format(
        "SELECT %s FROM %I",
        parts.join(", "),
        TABLE_NAME
      );
    }

    // Run all queries in parallel for performance
    const [
      totalRecordsResult,
      uniqueUdiseResult,
      uniqueStatesResult,
      uniqueDistrictsResult,
      uniqueBlocksResult,
      uniqueClustersResult,
      uniqueVillagesResult,
      totalStudentsResult,
      topStatesResult,
      topDistrictsResult,
      topBlocksResult,
      schoolsByCategoryResult,
      schoolsByManagementResult,
    ] = await Promise.all([
      // Total Records
      pool.query(format("SELECT COUNT(*) as count FROM %I", TABLE_NAME)),
      
      // Unique UDISE Codes
      pool.query(
        format(
          "SELECT COUNT(DISTINCT udise_code) as count FROM %I WHERE udise_code IS NOT NULL AND udise_code != 'NA'",
          TABLE_NAME
        )
      ),
      
      // Unique States
      pool.query(
        format(
          "SELECT COUNT(DISTINCT state) as count FROM %I WHERE state IS NOT NULL AND state != 'NA'",
          TABLE_NAME
        )
      ),
      
      // Unique Districts
      pool.query(
        format(
          "SELECT COUNT(DISTINCT district) as count FROM %I WHERE district IS NOT NULL AND district != 'NA'",
          TABLE_NAME
        )
      ),
      
      // Unique Blocks
      pool.query(
        format(
          "SELECT COUNT(DISTINCT block) as count FROM %I WHERE block IS NOT NULL AND block != 'NA'",
          TABLE_NAME
        )
      ),
      
      // Unique Clusters
      pool.query(
        format(
          "SELECT COUNT(DISTINCT cluster) as count FROM %I WHERE cluster IS NOT NULL AND cluster != 'NA'",
          TABLE_NAME
        )
      ),
      
      // Unique Villages
      pool.query(
        format(
          "SELECT COUNT(DISTINCT village) as count FROM %I WHERE village IS NOT NULL AND village != 'NA'",
          TABLE_NAME
        )
      ),
      
      // Total Students (using dynamic query built above)
      pool.query(studentSumQuery),
      
      // Top 5 States by school count
      pool.query(
        format(
          `SELECT state as name, COUNT(*) as count 
           FROM %I 
           WHERE state IS NOT NULL AND state != 'NA' 
           GROUP BY state 
           ORDER BY count DESC 
           LIMIT 5`,
          TABLE_NAME
        )
      ),
      
      // Top 5 Districts by school count
      pool.query(
        format(
          `SELECT district as name, COUNT(*) as count 
           FROM %I 
           WHERE district IS NOT NULL AND district != 'NA' 
           GROUP BY district 
           ORDER BY count DESC 
           LIMIT 5`,
          TABLE_NAME
        )
      ),
      
      // Top 5 Blocks by school count
      pool.query(
        format(
          `SELECT block as name, COUNT(*) as count 
           FROM %I 
           WHERE block IS NOT NULL AND block != 'NA' 
           GROUP BY block 
           ORDER BY count DESC 
           LIMIT 5`,
          TABLE_NAME
        )
      ),
      
      // Schools by Category
      pool.query(
        format(
          `SELECT school_category as category, COUNT(*) as count 
           FROM %I 
           WHERE school_category IS NOT NULL AND school_category != 'NA' 
           GROUP BY school_category 
           ORDER BY count DESC 
           LIMIT 10`,
          TABLE_NAME
        )
      ),
      
      // Schools by Management
      pool.query(
        format(
          `SELECT school_management as management, COUNT(*) as count 
           FROM %I 
           WHERE school_management IS NOT NULL AND school_management != 'NA' 
           GROUP BY school_management 
           ORDER BY count DESC 
           LIMIT 10`,
          TABLE_NAME
        )
      ),
    ]);

    const studentData = totalStudentsResult.rows[0] || { total: 0, boys: 0, girls: 0 };

    return {
      totalRecords: parseInt(totalRecordsResult.rows[0]?.count || 0),
      uniqueUdise: parseInt(uniqueUdiseResult.rows[0]?.count || 0),
      uniqueStates: parseInt(uniqueStatesResult.rows[0]?.count || 0),
      uniqueDistricts: parseInt(uniqueDistrictsResult.rows[0]?.count || 0),
      uniqueBlocks: parseInt(uniqueBlocksResult.rows[0]?.count || 0),
      uniqueClusters: parseInt(uniqueClustersResult.rows[0]?.count || 0),
      uniqueVillages: parseInt(uniqueVillagesResult.rows[0]?.count || 0),
      totalStudents: parseInt(studentData.total || 0),
      totalBoyStudents: parseInt(studentData.boys || 0),
      totalGirlStudents: parseInt(studentData.girls || 0),
      topStates: topStatesResult.rows.map(r => ({
        name: r.name,
        count: parseInt(r.count)
      })),
      topDistricts: topDistrictsResult.rows.map(r => ({
        name: r.name,
        count: parseInt(r.count)
      })),
      topBlocks: topBlocksResult.rows.map(r => ({
        name: r.name,
        count: parseInt(r.count)
      })),
      schoolsByCategory: schoolsByCategoryResult.rows.map(r => ({
        category: r.category,
        count: parseInt(r.count)
      })),
      schoolsByManagement: schoolsByManagementResult.rows.map(r => ({
        management: r.management,
        count: parseInt(r.count)
      })),
    };
  } catch (err) {
    console.error("Dashboard Stats Error:", err);
    throw err;
  }
};

// Add to module.exports
module.exports = {
  proxyUdiseRequest,
  saveSchoolsToDb,
  getFiltersFromDb,
  searchSchoolsInDb,
  getExistingCodes,
  getDashboardStats,  // Make sure this is exported
};