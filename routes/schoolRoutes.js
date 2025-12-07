const express = require('express');
const router = express.Router();
const schoolController = require('../controllers/schoolController');

// 1. Proxy Route
router.use('/udise', schoolController.proxyUdise);

// 2. DB Operations
router.post('/save-schools', schoolController.saveSchools);
router.get('/filters', schoolController.getFilters);
router.post('/schools/search', schoolController.searchSchools);

// 3. NEW: Check Existing
router.post('/check-existing', schoolController.checkExisting);

module.exports = router;