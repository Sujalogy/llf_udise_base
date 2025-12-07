const express = require('express');
const router = express.Router();
const schoolController = require('../controllers/schoolController');

// 1. Proxy Route
// Mounts at /api/udise. Any path after that is passed to the controller.
router.use('/udise', schoolController.proxyUdise);

// 2. DB Operations
router.post('/save-schools', schoolController.saveSchools);
router.get('/filters', schoolController.getFilters);
router.post('/schools/search', schoolController.searchSchools);

module.exports = router;