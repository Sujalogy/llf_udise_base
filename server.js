const express = require('express');
const cors = require('cors');
const pool = require('./db');
const schoolRoutes = require('./routes/schoolRoutes');

const app = express();
const PORT = 3000;

app.use(cors({ origin: ['http://localhost:5173', 'http://localhost:5174'], credentials: true }));
app.use(express.json({ limit: '50mb' }));

pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('❌ DATABASE CONNECTION FAILED:', err.message);
  } else {
    console.log('✅ DATABASE CONNECTED:', res.rows[0].now);
  }
});

app.use('/api', schoolRoutes);

// Token cleanup cron
setInterval(async () => {
  try {
    const authService = require('./services/authService');
    const deleted = await authService.cleanupExpiredTokens();
    console.log(`🗑️ Cleaned up ${deleted} expired tokens`);
  } catch (error) {
    console.error('Token cleanup failed:', error);
  }
}, 60 * 60 * 1000);

app.listen(PORT, () => {
  console.log(`🚀 Server: http://localhost:${PORT}`);
});