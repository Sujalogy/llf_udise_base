const { Pool } = require('pg');

// Database Configuration
const pool = new Pool({
  user: 'postgres',      // Your PostgreSQL username
  host: 'localhost',
  database: 'Udise_database', // Your Database name
  password: 'secret',    // Your Password
  port: 5432,
});

// Listener for unexpected errors on idle clients
pool.on('error', (err) => {
  console.error('Unexpected error on idle client', err);
  process.exit(-1);
});

module.exports = pool;