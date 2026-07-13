require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const amqp = require('amqplib');
const { Resend } = require('resend');
const { rateLimit } = require('express-rate-limit');
const cors = require('cors'); 
const NotificationLog = require('./models/NotificationLog');

const app = express();
app.set('trust proxy', 1);

const resend = new Resend(process.env.RESEND_API_KEY);

app.use(cors({ origin: '*', methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'], allowedHeaders: ['Content-Type', 'Authorization'], credentials: true }));
app.use(express.json());

const PORT = process.env.PORT || 5000; 
let rabbitChannel = null;

const MAIN_QUEUE = 'notifications_v5_queue'; 
const DLX_EXCHANGE = 'notification_dlx_v5';
const DLQ_FINAL = 'dead_letter_queue_v5';

const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/nexus_db';
mongoose.connect(MONGO_URI).then(() => console.log('[INFO] MongoDB connected.')).catch(err => console.error(err.message));

async function initRabbitMQ() {
    const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://127.0.0.1:5672';
    const connection = await amqp.connect(RABBITMQ_URL);
    rabbitChannel = await connection.createChannel();
    await rabbitChannel.assertExchange(DLX_EXCHANGE, 'direct', { durable: true });
    await rabbitChannel.assertQueue(DLQ_FINAL, { durable: true });
    await rabbitChannel.assertQueue(MAIN_QUEUE, { durable: true, arguments: { 'x-dead-letter-exchange': DLX_EXCHANGE, 'x-dead-letter-routing-key': DLQ_FINAL } });
    await rabbitChannel.bindQueue(DLQ_FINAL, DLX_EXCHANGE, DLQ_FINAL);
    console.log('[INFO] RabbitMQ initialized.');
}

async function startWorker() {
    console.log('[INFO] Worker active and listening...');
    rabbitChannel.prefetch(1);
    rabbitChannel.consume(MAIN_QUEUE, async (msg) => {
        if (msg !== null) {
            const { logId, userId, templateType, email } = JSON.parse(msg.content.toString());
            try {
                await resend.emails.send({
                    from: 'onboarding@resend.dev',
                    to: email || 'nikitachitalkar29@gmail.com',
                    subject: `Alert: ${templateType}`,
                    html: `<p>Hello ${userId}, your notification for [${templateType}] was processed successfully.</p>`
                });
                console.log('[INFO] Email sent successfully via Resend API.');
                if (logId) await NotificationLog.findByIdAndUpdate(logId, { status: 'SUCCESS' });
                rabbitChannel.ack(msg);
            } catch (error) {
                console.error('[ERROR] Resend API Error:', error);
                rabbitChannel.nack(msg, false, false);
            }
        }
    });
}

app.post('/api/v1/notifications/send', async (req, res) => {
    try {
        const { userId, channel, templateType, email } = req.body;
        const logEntry = await NotificationLog.create({ userId, channel, templateType, status: 'PENDING' });
        rabbitChannel.sendToQueue(MAIN_QUEUE, Buffer.from(JSON.stringify({ logId: logEntry._id, userId, channel, templateType, email })));
        res.status(202).json({ message: 'Queued', logId: logEntry._id });
    } catch (err) {
        res.status(500).json({ error: 'Queue failure' });
    }
});

async function startServer() {
    app.listen(PORT, async () => {
        await initRabbitMQ();
        await startWorker();
        console.log(`[INFO] Server running on port ${PORT}`);
    });
}

startServer();