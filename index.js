const express = require('express');
const cors = require('cors');
const cookieParser = require('cookie-parser');
require('dotenv').config();
const connectDB = require('./config/db');
const router = require('./routes/routes');
const path = require('path');

const app = express();

// Middleware
app.use(express.json()); // For parsing JSON request bodies
app.use(cookieParser()); // For parsing cookies

// Define allowed origins (you can add more domains if needed)
const allowedOrigins = [
  'https://mern-eccomerce-website-1-etxt.onrender.com',
  'https://mern-eccomerce-website-ya7m.onrender.com'
];

// CORS configuration
app.use(cors({
  origin: (origin, callback) => {
    // Allow requests with no origin (e.g., mobile apps, curl requests)
    if (!origin) return callback(null, true);
    if (allowedOrigins.includes(origin)) {
      return callback(null, true);
    } else {
      return callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: true // Enable credentials (cookies, authentication tokens, etc.)
}));

// Routes
app.use('/api', router);

const PORT = process.env.PORT || 8080;

// Serve static files in production (optional setup based on your needs)
// app.use(express.static(path.join(__dirname, 'client/build')));

// Connect to the database and start the server
connectDB().then(() => {
  app.listen(PORT, () => {
    console.log('Connected to the database');
    console.log(`Server is running on port ${PORT}`);
  });
}).catch((error) => {
  console.error('Failed to connect to the database', error);
});