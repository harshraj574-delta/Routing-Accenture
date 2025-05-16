const { Employee } = require('../models');

const employeeController = {
  async getAllEmployees(req, res) {
    try {
      const { shift } = req.query;
      
      // Create where clause based on shift
      const whereClause = shift ? { shift: parseInt(shift) } : {};
      
      console.log('Fetching employees with where clause:', whereClause);
      
      // First, try to get a single employee to test the connection
      const testEmployee = await Employee.findOne({
        attributes: ['empCode', 'shift']
      });
      
      console.log('Test employee:', testEmployee);
      
      const employees = await Employee.findAll({
        where: whereClause,
        attributes: ['empCode', 'address', 'geoX', 'geoY', 'gender', 'shift']
      });

      console.log(`Found ${employees.length} employees for shift ${shift}`);
      console.log('Sample employee:', employees[0]);

      if (employees.length === 0) {
        return res.status(404).json({ 
          error: `No employees found for shift ${shift}`,
          message: 'Please check if there are any employees assigned to this shift'
        });
      }

      const transformedEmployees = employees.map(emp => {
        return {
          id: emp.empCode,
          name: emp.empCode,
          location: {
            lat: parseFloat(emp.geoY),
            lng: parseFloat(emp.geoX)
          },
          address: emp.address || '',
          gender: emp.gender || 'unknown',
          shift: emp.shift,
          zone: emp.zone || ''
        };
      });

      res.json(transformedEmployees);
    } catch (error) {
      console.error('Error in getAllEmployees:', error);
      console.error('Error details:', {
        message: error.message,
        stack: error.stack,
        sql: error.sql,
        sqlMessage: error.sqlMessage
      });
      res.status(500).json({ 
        error: error.message,
        details: error.sqlMessage || 'Database error occurred'
      });
    }
  }
};

module.exports = employeeController; 