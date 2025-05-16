const { Facility } = require('../models');

// Get all facilities
exports.getAllFacilities = async (req, res) => {
  try {
    const facilities = await Facility.findAll();
    res.json(facilities);
  } catch (error) {
    console.error('Error fetching facilities:', error);
    res.status(500).json({ message: 'Error fetching facilities' });
  }
};

// Create a new facility
exports.createFacility = async (req, res) => {
  try {
    const { name, geoX, geoY } = req.body;
    const facility = await Facility.create({
      name,
      geoX,
      geoY
    });
    res.status(201).json(facility);
  } catch (error) {
    console.error('Error creating facility:', error);
    res.status(400).json({ message: 'Error creating facility' });
  }
};

// Get a single facility by ID
exports.getFacilityById = async (req, res) => {
  try {
    const facility = await Facility.findByPk(req.params.id);
    if (facility) {
      res.json(facility);
    } else {
      res.status(404).json({ message: 'Facility not found' });
    }
  } catch (error) {
    console.error('Error fetching facility:', error);
    res.status(500).json({ message: 'Error fetching facility' });
  }
};

// Update a facility
exports.updateFacility = async (req, res) => {
  try {
    const { name, geoX, geoY } = req.body;
    const facility = await Facility.findByPk(req.params.id);
    
    if (facility) {
      await facility.update({
        name: name || facility.name,
        geoX: geoX || facility.geoX,
        geoY: geoY || facility.geoY
      });
      res.json(facility);
    } else {
      res.status(404).json({ message: 'Facility not found' });
    }
  } catch (error) {
    console.error('Error updating facility:', error);
    res.status(400).json({ message: 'Error updating facility' });
  }
};

// Delete a facility
exports.deleteFacility = async (req, res) => {
  try {
    const facility = await Facility.findByPk(req.params.id);
    if (facility) {
      await facility.destroy();
      res.json({ message: 'Facility deleted successfully' });
    } else {
      res.status(404).json({ message: 'Facility not found' });
    }
  } catch (error) {
    console.error('Error deleting facility:', error);
    res.status(500).json({ message: 'Error deleting facility' });
  }
}; 