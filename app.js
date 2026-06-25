const express = require('express');
const mongoose = require('mongoose');
const amqp = require('amqplib');
const { createClient } = require('redis');
const { rateLimit } = require('express-rate-limit');
const { RedisStore } = require('rate-limit-redis');
const NotificationLog = require('./models/NotificationLog');

const app = express();
app.use(express.json());

const PORT = 3000;
let rabbitChannel = null;

// Constants for Advanced Resilient Topology
const DLX_EXCHANGE = 'notification_dlx_v3';
const RETRY_QUEUE = 'retry_queue_v3';
const MAIN_QUEUE = 'notifications_v3_queue';

// 1. Initialize Redis Client Instance
const redisClient = createClient({ url: 'redis://127.0.0.1:6379' });
redisClient.on('error', (err) => console.error('❌ Redis Client Error:', err));

// 2. Define Rate Limiting Rule cleanly
const apiLimiter = rateLimit({
    windowMs: 1 * 60 * 1000, // 1 minute window
    max: 10000, 
    standardHeaders: true, 
    legacyHeaders: false, 
    store: new RedisStore({
        sendCommand: async (...args) => {
            // Defensive check: if redis isn't ready yet, wait briefly or handle gracefully
            if (!redisClient.isOpen) {
                return 0; 
            }
            return redisClient.sendCommand(args);
        },
    }),
    handler: (req, res) => {
        res.status(429).json({
            error: 'Too Many Requests',
            message: 'You have exceeded the rate limit. Please try again later.'
        });
    }
});

// 3. Connect to MongoDB
mongoose.connect('mongodb://localhost:27017/nexus_db')
  .then(() => console.log('✅ MongoDB Connected Successfully!'))
  .catch((err) => console.error('❌ MongoDB Connection Error:', err));

// 4. Connect to RabbitMQ with automatic retry if it's warming up
async function initRabbitMQ() {
    const maxRetries = 3;
    for (let i = 1; i <= maxRetries; i++) {
        try {
            console.log(`⏳ Connecting to RabbitMQ (Attempt ${i}/${maxRetries})...`);
            const connection = await amqp.connect('amqp://127.0.0.1:5672'); 
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
// 6. Sequential, Reliable Server Bootup Sequence
async function startServer() {
    try {
        console.log('⏳ Connecting to Redis...');
        await redisClient.connect();
        console.log('✅ Redis Connected Successfully for Rate Limiting!');

        // Humne isko try-catch mein daal diya taaki error aaye toh bhi server na ruke!
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