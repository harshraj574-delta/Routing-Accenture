const { Profile } = require('../models');

const profileController = {
  // Create a new profile
  async create(req, res) {
    try {
      const profile = await Profile.create(req.body);
      res.status(201).json(profile);
    } catch (error) {
      res.status(400).json({ error: error.message });
    }
  },

  // Get all profiles
  async getAll(req, res) {
    try {
      const profiles = await Profile.findAll();
      res.json(profiles);
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  },

  // Get profile by ID
  async getById(req, res) {
    try {
      const profile = await Profile.findByPk(req.params.id);
      if (!profile) {
        return res.status(404).json({ error: 'Profile not found' });
      }
      res.json(profile);
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  },

  // Update profile
  async update(req, res) {
    try {
      const profile = await Profile.findByPk(req.params.id);
      if (!profile) {
        return res.status(404).json({ error: 'Profile not found' });
      }
      await profile.update(req.body);
      res.json(profile);
    } catch (error) {
      res.status(400).json({ error: error.message });
    }
  },

  // Delete profile
  async delete(req, res) {
    try {
      const profile = await Profile.findByPk(req.params.id);
      if (!profile) {
        return res.status(404).json({ error: 'Profile not found' });
      }
      await profile.destroy();
      res.status(204).send();
    } catch (error) {
      res.status(500).json({ error: error.message });
    }
  }
};

module.exports = profileController;