const express = require('express');
const mongoose = require('mongoose');
const amqp = require('amqplib');
const { createClient } = require('redis');
const { rateLimit } = require('express-rate-limit');
const { RedisStore } = require('rate-limit-redis');
const NotificationLog = require('./models/NotificationLog');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
let rabbitChannel = null;

// Constants for Advanced Resilient Topology
const DLX_EXCHANGE = 'notification_dlx_v3';
const RETRY_QUEUE = 'retry_queue_v3';
const MAIN_QUEUE = 'notifications_v3_queue';

// 1. Initialize Redis Client Instance with cloud fallback URL
const redisClient = createClient({ 
    url: process.env.REDIS_URL || 'redis://127.0.0.1:6379' 
});
redisClient.on('error', (err) => console.error('❌ Redis Client Error:', err));

// 2. Define Rate Limiting Rule cleanly
const apiLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute window
    max: 10000, 
    standardHeaders: true, 
    legacyHeaders: false, 
    store: new RedisStore({
        sendCommand: async (...args) => {
            // Defensive check: if redis isn't ready or connected, gracefully bypass to prevent app crash
            if (!redisClient.isOpen) {
                return 0; 
            }
            try {
                return await redisClient.sendCommand(args);
            } catch (err) {
                console.error('⚠️ Rate limit redis store command failed:', err.message);
                return 0;
            }
        },
    }),
    handler: (req, res) => {
        res.status(429).json({
            error: 'Too Many Requests',
            message: 'You have exceeded the rate limit. Please try again later.'
        });
    }
});

// 3. Connect to MongoDB using environment variable
const mongoURI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/nexus_db';
mongoose.connect(mongoURI)
  .then(() => console.log('✅ MongoDB Connected Successfully!'))
  .catch((err) => console.error('❌ MongoDB Connection Error:', err));

// 4. Connect to RabbitMQ with automatic retry if it's warming up
async function initRabbitMQ() {
    const rabbitURL = process.env.RABBITMQ_URL || 'amqp://127.0.0.1:5672';
    const maxRetries = 3;
    for (let i = 1; i <= maxRetries; i++) {
        try {
            console.log(`⏳ Connecting to RabbitMQ (Attempt ${i}/${maxRetries})...`);
            const connection = await amqp.connect(rabbitURL); 
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
            console.log('✅ RabbitMQ Topologies & Resilient Queues Initialized!');
            return; // Success! Break out of retry loop
        } catch (error) {
            console.error(`⚠️ RabbitMQ connection attempt ${i} failed: ${error.message}`);
            if (i === maxRetries) throw error;
            // Wait 4 seconds before retrying to let the container finish booting up
            await new Promise((resolve) => setTimeout(resolve, 4000));
        }
    }
}

// 5. API Endpoint equipped with Redis Rate Limiter Middleware
app.post('/api/v1/notifications/send', apiLimiter, async (req, res) => {
    try {
        const { userId, channel, templateType } = req.body;

        if (!userId || !channel || !templateType) {
            return res.status(400).json({ error: 'Missing mandatory fields' });
        }

        const logEntry = await NotificationLog.create({
            userId,
            channel,
            templateType,
            status: 'PENDING'
        });

        const messagePayload = {
            logId: logEntry._id,
            userId,
            channel,
            templateType
        };

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

// 6. Sequential, Reliable Server Bootup Sequence
async function startServer() {
    try {
        console.log('⏳ Connecting to Redis...');
        try {
            await redisClient.connect();
            console.log('✅ Redis Connected Successfully for Rate Limiting!');
        } catch (redisError) {
            console.error('⚠️ Redis connection failed. Bypassing rate-limiter store to avoid crash:', redisError.message);
        }

        // RabbitMQ implementation in try-catch to allow independent execution flow
        try {
            await initRabbitMQ();
        } catch (mqError) {
            console.error('⚠️ RabbitMQ bypass kiya (Cloud setup missing):', mqError.message);
        }

        app.listen(PORT, () => {
            console.log(`🚀 Resilient Server running on port ${PORT}`);
        });
    } catch (error) {
        console.error('❌ Critical Server Startup Failure:', error.message);
        process.exit(1);
    }
}

startServer();