const schoolService = require('../services/schoolService');

const proxyUdise = async (req, res) => {
  try {
    // req.url here comes from the router.use('/udise')
    // It will be whatever is AFTER /api/udise
    const result = await schoolService.proxyUdiseRequest(req.method, req.url, req.body);
    res.json(result);
  } catch (error) {
    if (error.status) {
      res.status(error.status).json(error.data || { message: error.message });
    } else {
      res.status(500).json({ message: 'Internal Server Error', error: error.message });
    }
  }
};

const saveSchools = async (req, res) => {
  try {
    const schoolsData = req.body;
    if (!Array.isArray(schoolsData) || schoolsData.length === 0) {
      return res.status(400).json({ message: 'No data provided' });
    }
    const result = await schoolService.saveSchoolsToDb(schoolsData);
    res.json({ success: true, message: `Saved ${result.count} records.`, count: result.count });
  } catch (err) {
    console.error('Controller Error:', err);
    res.status(500).json({ success: false, message: 'Database error', error: err.message });
  }
};

const getFilters = async (req, res) => {
  try {
    const filters = await schoolService.getFiltersFromDb();
    res.json(filters);
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch filters' });
  }
};

const searchSchools = async (req, res) => {
  try {
    const { state, districts } = req.body;
    const data = await schoolService.searchSchoolsInDb(state, districts);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: 'Failed to fetch data' });
  }
};

module.exports = { proxyUdise, saveSchools, getFilters, searchSchools };