const express = require('express');
const mongoose = require('mongoose');
const amqp = require('amqplib');
const { createClient } = require('redis');
const { rateLimit } = require('express-rate-limit');
const cors = require('cors'); 
const NotificationLog = require('./models/NotificationLog');

const app = express();

// 🚀 1. CORS Headers globally enable kiye (Automatically handles Pre-flight OPTIONS)
app.use(cors({
    origin: '*', 
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: true
}));

app.use(express.json());

const PORT = process.env.PORT || 3000; 
let rabbitChannel = null;

const DLX_EXCHANGE = 'notification_dlx_v3';
const RETRY_QUEUE = 'retry_queue_v3';
const MAIN_QUEUE = 'notifications_v3_queue';

// 2. Safe Localized Memory-Based Rate Limiter (No External Database Dependency to Crash)
const apiLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute
    max: 10000, 
    standardHeaders: true, 
    legacyHeaders: false,
    handler: (req, res) => {
        res.status(429).json({
            error: 'Too Many Requests',
            message: 'You have exceeded the rate limit. Please try again later.'
        });
    }
});

// 3. Connect to MongoDB (Bulletproof Timeout Config)
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/nexus_db';
mongoose.connect(MONGO_URI, {
    serverSelectionTimeoutMS: 4000 
})
  .then(() => console.log('✅ MongoDB Connected Successfully!'))
  .catch((err) => console.error('⚠️ MongoDB bypass kiya (Cloud Connection Offline):', err.message));

// 4. Connect to RabbitMQ Safely
async function initRabbitMQ() {
    const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://127.0.0.1:5672';
    try {
        console.log(`⏳ Connecting to RabbitMQ...`);
        const connection = await amqp.connect(RABBITMQ_URL); 
        rabbitChannel = await connection.createChannel();

        await rabbitChannel.assertExchange(DLX_EXCHANGE, 'direct', { durable: true });
        await rabbitChannel.assertQueue(RETRY_QUEUE, {
            durable: true,
            arguments: {
                'x-dead-letter-exchange': '',
                'x-dead-letter-routing-key': MAIN_QUEUE,
                'x-message-ttl': 5000
            }
        });
        await rabbitChannel.assertQueue(MAIN_QUEUE, {
            durable: true,
            arguments: {
                'x-dead-letter-exchange': DLX_EXCHANGE,
                'x-dead-letter-routing-key': RETRY_QUEUE
            }
        });
        await rabbitChannel.bindQueue(RETRY_QUEUE, DLX_EXCHANGE, RETRY_QUEUE);
        console.log('✅ RabbitMQ Topologies Initialized!');
    } catch (error) {
        console.error(`⚠️ RabbitMQ Setup Bypassed cleanly: ${error.message}`);
    }
}

// Dummy Route for Render Health Check - Fix for Express v5 Wildcard Engine
app.get('/', (req, res) => {
    res.send('🚀 Resilient Nexus Notify Server is Live and Running!');
});

// 5. Main POST API Endpoint
app.post('/api/v1/notifications/send', apiLimiter, async (req, res) => {
    try {
        const { userId, channel, templateType } = req.body;

        if (!userId || !channel || !templateType) {
            return res.status(400).json({ error: 'Missing mandatory fields' });
        }

        // Production isolated handling: Database connection missing rule
        if (mongoose.connection.readyState !== 1) {
            return res.status(503).json({ error: 'Database service is currently unavailable offline.' });
        }

        const logEntry = await NotificationLog.create({
            userId,
            channel,
            templateType,
            status: 'PENDING'
        });

        const messagePayload = { logId: logEntry._id, userId, channel, templateType };

        if (rabbitChannel) {
            rabbitChannel.sendToQueue(MAIN_QUEUE, Buffer.from(JSON.stringify(messagePayload)), {
                persistent: true
            });
            return res.status(202).json({
                message: 'Notification request accepted and queued.',
                logId: logEntry._id
            });
        } else {
            throw new Error('RabbitMQ channel is unavailable');
        }

    } catch (error) {
        console.error('❌ API Error:', error.message);
        res.status(500).json({ error: 'Internal Server Error', details: error.message });
    }
});

// Server Boot Sequence
async function startServer() {
    app.listen(PORT, () => {
        console.log(`🚀 Resilient Server successfully running on port ${PORT}`);
    });
    
    // RabbitMQ initialization safely in separate execution thread
    initRabbitMQ();
}

startServer();