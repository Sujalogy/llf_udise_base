const pool = require("../db");

const findUserByEmail = async (email) => {
  const result = await pool.query("SELECT * FROM users WHERE email = $1", [email]);
  return result.rows[0];
};

const findUserById = async (userId) => {
  const result = await pool.query("SELECT * FROM users WHERE user_id = $1", [userId]);
  return result.rows[0];
};

const createUser = async ({ email, google_id, name, profile_picture, role, status }) => {
  const result = await pool.query(
    `INSERT INTO users (email, google_id, name, profile_picture, role, status, last_login)
     VALUES ($1, $2, $3, $4, $5, $6, NOW()) RETURNING *`,
    [email, google_id, name, profile_picture, role || 'user', status || 'active']
  );
  return result.rows[0];
};

const updateUserLogin = async (userId, { google_id, name, profile_picture }) => {
  await pool.query(
    `UPDATE users SET google_id = $1, name = $2, profile_picture = $3, 
     last_login = NOW(), updated_at = NOW() WHERE user_id = $4`,
    [google_id, name, profile_picture, userId]
  );
};

const createAuthToken = async ({ user_id, token, expires_at }) => {
  await pool.query(
    "INSERT INTO auth_tokens (user_id, token, expires_at) VALUES ($1, $2, $3)",
    [user_id, token, expires_at]
  );
};

const verifyToken = async (token) => {
  const result = await pool.query(
    `SELECT t.*, u.user_id, u.email, u.name, u.role, u.status
     FROM auth_tokens t
     JOIN users u ON t.user_id = u.user_id
     WHERE t.token = $1 AND t.expires_at > NOW() AND u.status = 'active'`,
    [token]
  );
  
  if (result.rows.length === 0) return null;
  
  return {
    token: result.rows[0].token,
    user: {
      user_id: result.rows[0].user_id,
      email: result.rows[0].email,
      name: result.rows[0].name,
      role: result.rows[0].role,
      status: result.rows[0].status
    }
  };
};

const deleteToken = async (token) => {
  await pool.query("DELETE FROM auth_tokens WHERE token = $1", [token]);
};

const cleanupExpiredTokens = async () => {
  const result = await pool.query("DELETE FROM auth_tokens WHERE expires_at < NOW()");
  return result.rowCount;
};

const getAllUsers = async ({ page = 1, limit = 50, search = "", role = "" }) => {
  const offset = (page - 1) * limit;
  let query = `SELECT user_id, email, name, role, status, profile_picture, last_login, created_at FROM users WHERE 1=1`;
  const params = [];
  let paramCount = 1;

  if (search) {
    query += ` AND (email ILIKE $${paramCount} OR name ILIKE $${paramCount})`;
    params.push(`%${search}%`);
    paramCount++;
  }

  if (role) {
    query += ` AND role = $${paramCount}`;
    params.push(role);
    paramCount++;
  }

  query += ` ORDER BY created_at DESC LIMIT $${paramCount} OFFSET $${paramCount + 1}`;
  params.push(limit, offset);

  const result = await pool.query(query, params);
  
  const countQuery = `SELECT COUNT(*) FROM users WHERE 1=1 ${search ? `AND (email ILIKE '%${search}%' OR name ILIKE '%${search}%')` : ''} ${role ? `AND role = '${role}'` : ''}`;
  const countResult = await pool.query(countQuery);

  return {
    users: result.rows,
    total: parseInt(countResult.rows[0].count),
    page,
    totalPages: Math.ceil(countResult.rows[0].count / limit)
  };
};

const updateUser = async (userId, { role, status }) => {
  const updates = [];
  const params = [];
  let paramCount = 1;

  if (role) {
    updates.push(`role = $${paramCount++}`);
    params.push(role);
  }

  if (status) {
    updates.push(`status = $${paramCount++}`);
    params.push(status);
  }

  updates.push(`updated_at = NOW()`);
  params.push(userId);

  const query = `UPDATE users SET ${updates.join(', ')} WHERE user_id = $${paramCount} RETURNING *`;
  const result = await pool.query(query, params);
  return result.rows[0];
};

const deleteUser = async (userId) => {
  await pool.query("DELETE FROM users WHERE user_id = $1", [userId]);
};

module.exports = {
  findUserByEmail,
  findUserById,
  createUser,
  updateUserLogin,
  createAuthToken,
  verifyToken,
  deleteToken,
  cleanupExpiredTokens,
  getAllUsers,
  updateUser,
  deleteUser
};