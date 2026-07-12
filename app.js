require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const amqp = require('amqplib');
const { rateLimit } = require('express-rate-limit');
const cors = require('cors'); 
const NotificationLog = require('./models/NotificationLog');

const app = express();

app.use(cors({
    origin: '*', 
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
    credentials: true
}));

app.use(express.json());

const PORT = process.env.PORT || 5000; 
let rabbitChannel = null;

// Topology Config
const DLX_EXCHANGE = 'notification_dlx_v5';
const DLQ_FINAL = 'dead_letter_queue_v5';
const MAIN_QUEUE = 'notifications_v5_queue';

const apiLimiter = rateLimit({
    windowMs: 1 * 60 * 1000,
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

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/nexus_db';
mongoose.connect(MONGO_URI, {
    serverSelectionTimeoutMS: 4000 
})
  .then(() => console.log('✅ MongoDB Connected Successfully!'))
  .catch((err) => console.error('⚠️ MongoDB bypass (Cloud Offline):', err.message));

async function initRabbitMQ() {
    const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://127.0.0.1:5672';
    try {
        console.log(`⏳ Connecting to RabbitMQ...`);
        const connection = await amqp.connect(RABBITMQ_URL); 
        rabbitChannel = await connection.createChannel();
            
        // 1. Setup DLX
        await rabbitChannel.assertExchange(DLX_EXCHANGE, 'direct', { durable: true });
        
        // 2. Setup Final DLQ
        await rabbitChannel.assertQueue(DLQ_FINAL, { durable: true });
        
        // 3. Setup Main Queue linked with DLX
        await rabbitChannel.assertQueue(MAIN_QUEUE, {
            durable: true,
            arguments: {
                'x-dead-letter-exchange': DLX_EXCHANGE,
                'x-dead-letter-routing-key': DLQ_FINAL
            }
        });
        
        // 4. Bind DLQ to DLX
        await rabbitChannel.bindQueue(DLQ_FINAL, DLX_EXCHANGE, DLQ_FINAL);

        console.log('✅ RabbitMQ Topologies Initialized Successfully!');
    } catch (error) {
        console.error(`⚠️ RabbitMQ Setup Error: ${error.message}`);
    }
}

app.get('/', (req, res) => {
    res.send('🚀 Resilient Nexus Notify Server is Live and Running!');
});

app.post('/api/v1/notifications/send', apiLimiter, async (req, res) => {
    try {
        const { userId, channel, templateType, email } = req.body;

        if (!userId || !channel || !templateType) {
            return res.status(400).json({ error: 'Missing mandatory fields' });
        }

        if (mongoose.connection.readyState !== 1) {
            return res.status(503).json({ error: 'Database service is currently unavailable offline.' });
        }

        const logEntry = await NotificationLog.create({
            userId,
            channel,
            templateType,
            status: 'PENDING'
        });

        // Dynamic email payload passed forward
        const messagePayload = { logId: logEntry._id, userId, channel, templateType, email };

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

async function startServer() {
    app.listen(PORT, () => {
        console.log(`🚀 Resilient Server running on port ${PORT}`);
    });
    
    await initRabbitMQ();

    try {
        console.log('🔄 Booting background worker processor internally...');
        //require('./worker.js');
    } catch (workerInitError) {
        console.error('⚠️ Worker bootup failed:', workerInitError.message);
    }
}

startServer();