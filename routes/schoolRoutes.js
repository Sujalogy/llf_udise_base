const express = require('express');
const router = express.Router();
const schoolController = require('../controllers/schoolController');
const authController = require('../controllers/authController');
const userController = require('../controllers/userController');

// PUBLIC AUTH ROUTES
router.post('/auth/google', authController.googleAuth);
router.post('/auth/logout', authController.logout);
router.get('/auth/cleanup-tokens', authController.cleanupTokens);

// PROTECTED ROUTES (require authentication)
router.use(authController.verifyToken);

// Profile
router.get('/auth/profile', authController.getProfile);

// School Routes
router.use('/udise', schoolController.proxyUdise);
router.post('/save-schools', schoolController.saveSchools);
router.get('/filters', schoolController.getFilters);
router.post('/schools/search', schoolController.searchSchools);
router.post('/check-existing', schoolController.checkExisting);
router.get('/dashboard/stats', schoolController.getDashboardStats);
router.get('/academic-years', schoolController.getAcademicYears);
router.get('/filter-options', schoolController.getAllFilterOptions);

// User Management (Admin only)
router.get('/users', userController.getUsers);
router.get('/users/stats', userController.getUserStats);
router.put('/users/:userId', userController.updateUser);
router.delete('/users/:userId', userController.deleteUser);

module.exports = router;