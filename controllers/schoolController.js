// ============================================================================
// --- FILE: controllers/schoolController.js ---
// ============================================================================
const schoolService = require("../services/schoolService");

const proxyUdise = async (req, res) => {
  try {
    const result = await schoolService.proxyUdiseRequest(
      req.method,
      req.url,
      req.body
    );
    res.json(result);
  } catch (error) {
    if (error.status) {
      res.status(error.status).json(error.data || { message: error.message });
    } else {
      res
        .status(500)
        .json({ message: "Internal Server Error", error: error.message });
    }
  }
};

const saveSchools = async (req, res) => {
  try {
    const schoolsData = req.body;
    if (!Array.isArray(schoolsData) || schoolsData.length === 0) {
      return res.status(400).json({ message: "No data provided" });
    }
    const result = await schoolService.saveSchoolsToDb(schoolsData);
    res.json({
      success: true,
      message: `Saved ${result.count} records.`,
      count: result.count,
    });
  } catch (err) {
    console.error("Controller Error:", err);
    if (err.code === "23505") {
      return res.json({
        success: true,
        message: "Some records were skipped (already exist).",
        count: 0,
      });
    }
    res
      .status(500)
      .json({ success: false, message: "Database error", error: err.message });
  }
};

const getFilters = async (req, res) => {
  try {
    const filters = await schoolService.getFiltersFromDb();
    res.json(filters);
  } catch (err) {
    res.status(500).json({ error: "Failed to fetch filters" });
  }
};

// --- UPDATED: Search with Pagination ---
const searchSchools = async (req, res) => {
  try {
    const { state, districts, page, limit } = req.body;
    const pageNum = parseInt(page) || 1;
    const limitNum = parseInt(limit) || 50;

    const result = await schoolService.searchSchoolsInDb(state, districts, pageNum, limitNum);
    
    // Result contains { data, total, page, limit }
    res.json(result); 
  } catch (err) {
    res.status(500).json({ error: "Failed to fetch data" });
  }
};

const checkExisting = async (req, res) => {
  try {
    const { codes, ay } = req.body; 
    if (!codes || !Array.isArray(codes))
      return res.status(400).json({ error: "Invalid codes array" });

    const existingCodes = await schoolService.getExistingCodes(codes, ay);
    res.json({ existing: existingCodes });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "Failed to check existing records" });
  }
};

const getDashboardStats = async (req, res) => {
  try {
    const { state, district, block, ay } = req.query;
    const stats = await schoolService.getDashboardStats({ state, district, block, ay });
    res.json(stats);
  } catch (err) {
    console.error("Dashboard Controller Error:", err);
    res.status(500).json({ error: "Failed to fetch dashboard statistics", message: err.message });
  }
};

const getAcademicYears = async (req, res) => {
  try {
    const years = await schoolService.getAcademicYears();
    res.json({ success: true, academicYears: years });
  } catch (err) {
    console.error("Get Academic Years Error:", err);
    res.status(500).json({ error: "Failed to fetch academic years" });
  }
};

const getAllFilterOptions = async (req, res) => {
  try {
    const options = await schoolService.getAllFilterOptions();
    res.json({ success: true, ...options });
  } catch (err) {
    console.error("Get Filter Options Error:", err);
    res.status(500).json({ error: "Failed to fetch filter options" });
  }
};

module.exports = {
  proxyUdise,
  saveSchools,
  getFilters,
  searchSchools,
  checkExisting,
  getDashboardStats,
  getAcademicYears,
  getAllFilterOptions
};