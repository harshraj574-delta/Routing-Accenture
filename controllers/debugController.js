const debugController = {
  getRoutes: async (req, res) => {
    try {
      res.json({ 
        message: 'Debug routes endpoint reached successfully',
        timestamp: new Date(),
        method: req.method,
        url: req.url,
        params: req.params,
        query: req.query
      });
    } catch (error) {
      console.error('Error in debug controller:', error);
      res.status(500).json({ error: 'Debug controller error' });
    }
  }
};

module.exports = debugController; 