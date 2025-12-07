const express = require('express');
const cors = require('cors');
const pool = require('./db');
const schoolRoutes = require('./routes/schoolRoutes');

const app = express();
const PORT = 3000;

// --- Middleware ---
app.use(cors());
app.use(express.json({ limit: '50mb' }));

// --- Database Connection Check ---
pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('------------------------------------------------');
    console.error('❌ DATABASE CONNECTION FAILED');
    console.error('Error:', err.message);
    console.error('------------------------------------------------');
  } else {
    console.log('------------------------------------------------');
    console.log('✅ DATABASE CONNECTED SUCCESSFULLY');
    console.log(`Timestamp: ${res.rows[0].now}`);
    console.log('------------------------------------------------');
  }
});

// --- Mount Routes ---
// This will prefix all routes in schoolRoutes with /api
// e.g., /api/udise, /api/save-schools
app.use('/api', schoolRoutes);

// --- Start Server ---
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});