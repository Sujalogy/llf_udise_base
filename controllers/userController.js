const authService = require("../services/authService");

const getUsers = async (req, res) => {
  try {
    if (req.user.role !== 'admin' && req.user.role !== 'super_admin') {
      return res.status(403).json({ success: false, message: "Unauthorized: Admin access required" });
    }

    const { page = 1, limit = 50, search = "", role = "" } = req.query;

    const result = await authService.getAllUsers({
      page: parseInt(page),
      limit: parseInt(limit),
      search,
      role
    });

    res.json({ success: true, ...result });
  } catch (error) {
    console.error("Get Users Error:", error);
    res.status(500).json({ success: false, message: "Failed to fetch users" });
  }
};

const updateUser = async (req, res) => {
  try {
    if (req.user.role !== 'admin' && req.user.role !== 'super_admin') {
      return res.status(403).json({ success: false, message: "Unauthorized: Admin access required" });
    }

    const { userId } = req.params;
    const { role, status } = req.body;

    if (parseInt(userId) === req.user.user_id && role && role !== req.user.role) {
      return res.status(400).json({ success: false, message: "Cannot modify your own role" });
    }

    const updatedUser = await authService.updateUser(userId, { role, status });

    res.json({ success: true, message: "User updated successfully", user: updatedUser });
  } catch (error) {
    console.error("Update User Error:", error);
    res.status(500).json({ success: false, message: "Failed to update user" });
  }
};

const deleteUser = async (req, res) => {
  try {
    if (req.user.role !== 'super_admin') {
      return res.status(403).json({ success: false, message: "Unauthorized: Super Admin access required" });
    }

    const { userId } = req.params;

    if (parseInt(userId) === req.user.user_id) {
      return res.status(400).json({ success: false, message: "Cannot delete your own account" });
    }

    await authService.deleteUser(userId);

    res.json({ success: true, message: "User deleted successfully" });
  } catch (error) {
    console.error("Delete User Error:", error);
    res.status(500).json({ success: false, message: "Failed to delete user" });
  }
};

const getUserStats = async (req, res) => {
  try {
    if (req.user.role !== 'admin' && req.user.role !== 'super_admin') {
      return res.status(403).json({ success: false, message: "Unauthorized" });
    }

    const pool = require("../db");
    const result = await pool.query(`
      SELECT 
        role,
        COUNT(*) as total_users,
        COUNT(CASE WHEN status = 'active' THEN 1 END) as active_users
      FROM users
      GROUP BY role
    `);

    res.json({ success: true, stats: result.rows });
  } catch (error) {
    console.error("Get User Stats Error:", error);
    res.status(500).json({ success: false, message: "Failed to fetch user statistics" });
  }
};

module.exports = {
  getUsers,
  updateUser,
  deleteUser,
  getUserStats
};