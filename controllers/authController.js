const authService = require("../services/authService");
const crypto = require("crypto");

const generateToken = () => {
  return crypto.randomBytes(32).toString("hex");
};

const googleAuth = async (req, res) => {
  try {
    const { email, name, picture, googleId } = req.body;

    if (!email || !googleId) {
      return res.status(400).json({ success: false, message: "Missing required fields" });
    }

    let user = await authService.findUserByEmail(email);
    
    if (!user) {
      user = await authService.createUser({
        email,
        google_id: googleId,
        name,
        profile_picture: picture,
        role: email.endsWith('@languageandlearningfoundation.org') ? 'user' : 'user',
        status: 'active'
      });
    } else {
      await authService.updateUserLogin(user.user_id, { google_id: googleId, name, profile_picture: picture });
    }

    const token = generateToken();
    const expiresAt = new Date(Date.now() + 24 * 60 * 60 * 1000);

    await authService.createAuthToken({ user_id: user.user_id, token, expires_at: expiresAt });

    res.json({
      success: true,
      token,
      user: {
        user_id: user.user_id,
        email: user.email,
        name: user.name,
        role: user.role,
        profile_picture: user.profile_picture
      },
      expiresAt
    });
  } catch (error) {
    console.error("Google Auth Error:", error);
    res.status(500).json({ success: false, message: "Authentication failed", error: error.message });
  }
};

const verifyToken = async (req, res, next) => {
  try {
    const token = req.headers.authorization?.replace("Bearer ", "");

    if (!token) {
      return res.status(401).json({ success: false, message: "No token provided" });
    }

    const authToken = await authService.verifyToken(token);

    if (!authToken) {
      return res.status(401).json({ success: false, message: "Invalid or expired token" });
    }

    req.user = authToken.user;
    next();
  } catch (error) {
    console.error("Token Verification Error:", error);
    res.status(401).json({ success: false, message: "Authentication failed" });
  }
};

const logout = async (req, res) => {
  try {
    const token = req.headers.authorization?.replace("Bearer ", "");
    if (token) await authService.deleteToken(token);
    res.json({ success: true, message: "Logged out successfully" });
  } catch (error) {
    console.error("Logout Error:", error);
    res.status(500).json({ success: false, message: "Logout failed" });
  }
};

const getProfile = async (req, res) => {
  try {
    const user = await authService.findUserById(req.user.user_id);
    if (!user) return res.status(404).json({ success: false, message: "User not found" });

    res.json({
      success: true,
      user: {
        user_id: user.user_id,
        email: user.email,
        name: user.name,
        role: user.role,
        profile_picture: user.profile_picture,
        status: user.status,
        last_login: user.last_login,
        created_at: user.created_at
      }
    });
  } catch (error) {
    console.error("Get Profile Error:", error);
    res.status(500).json({ success: false, message: "Failed to fetch profile" });
  }
};

const cleanupTokens = async (req, res) => {
  try {
    const deleted = await authService.cleanupExpiredTokens();
    res.json({ success: true, message: `Cleaned up ${deleted} expired tokens` });
  } catch (error) {
    console.error("Cleanup Error:", error);
    res.status(500).json({ success: false, message: "Cleanup failed" });
  }
};

module.exports = {
  googleAuth,
  verifyToken,
  logout,
  getProfile,
  cleanupTokens
};