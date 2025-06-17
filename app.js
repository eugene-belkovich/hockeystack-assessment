// require modules
const mongoose = require('mongoose');

const dotenv = require('dotenv');
const path = require('path');
dotenv.config({path: path.join(__dirname, '.env')});

const initializeApp = async () => {
  try {
    const {MONGO_URI} = process.env;

    const packageJson = require('./package.json');
    process.env.VERSION = packageJson.version;
    process.env.instance = 'app';

    mongoose.set('strictQuery', false);

    console.log('Connecting to database...');

    await mongoose.connect(MONGO_URI);

    console.log('Connected to database successfully');

    require('./Domain');

    console.log('Starting worker...');
    require('./worker')();

    console.log('Starting server...');
    require('./server');

    console.log('Application initialized successfully');
  } catch (error) {
    console.error('Failed to initialize application:', error);
    process.exit(1);
  }
};

const gracefulShutdown = async signal => {
  console.log(`Received ${signal}. Starting graceful shutdown...`);

  try {
    await mongoose.connection.close();
    console.log('Database connection closed');

    console.log('Graceful shutdown completed');
    process.exit(0);
  } catch (error) {
    console.error('Error during graceful shutdown:', error);
    process.exit(1);
  }
};

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

process.on('uncaughtException', error => {
  console.error('Uncaught Exception:', error);
  process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
  process.exit(1);
});

initializeApp();
