require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const amqp = require('amqplib');
const nodemailer = require('nodemailer'); // Ye zaroori hai
const { rateLimit } = require('express-rate-limit');
const cors = require('cors'); 
const NotificationLog = require('./models/NotificationLog');

const app = express();
app.set('trust proxy', 1);

app.use(cors({ origin: '*', methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'], allowedHeaders: ['Content-Type', 'Authorization'], credentials: true }));
app.use(express.json());

const PORT = process.env.PORT || 5000; 
let rabbitChannel = null;

// Configurations
const MAIN_QUEUE = 'notifications_v5_queue'; 
const DLX_EXCHANGE = 'notification_dlx_v5';
const DLQ_FINAL = 'dead_letter_queue_v5';

// Nodemailer setup
const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: { user: process.env.EMAIL_USER, pass: process.env.EMAIL_PASS }
});

// MongoDB & RabbitMQ Setup
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

// WORKER LOGIC MERGED HERE
async function startWorker() {
    console.log('[INFO] Worker active and listening...');
    rabbitChannel.prefetch(1);
    rabbitChannel.consume(MAIN_QUEUE, async (msg) => {
        if (msg !== null) {
            const { logId, userId, templateType, email } = JSON.parse(msg.content.toString());
            try {
                const mailOptions = { from: process.env.EMAIL_USER, to: email || 'nikitachitalkar29@gmail.com', subject: `Alert: ${templateType}`, text: `Hello ${userId}, your notification is ready.` };
                await transporter.sendMail(mailOptions);
                console.log('[INFO] Email sent successfully.');
                if (logId) await NotificationLog.findByIdAndUpdate(logId, { status: 'SUCCESS' });
                rabbitChannel.ack(msg);
            } catch (error) {
                console.error('[ERROR] Processing failed:', error.message);
                rabbitChannel.nack(msg, false, false);
            }
        }
    });
}

app.post('/api/v1/notifications/send', async (req, res) => {
    const { userId, channel, templateType, email } = req.body;
    const logEntry = await NotificationLog.create({ userId, channel, templateType, status: 'PENDING' });
    rabbitChannel.sendToQueue(MAIN_QUEUE, Buffer.from(JSON.stringify({ logId: logEntry._id, userId, channel, templateType, email })));
    res.status(202).json({ message: 'Queued', logId: logEntry._id });
});

async function startServer() {
    app.listen(PORT, async () => {
        await initRabbitMQ();
        await startWorker(); // Worker yahan start hoga
        console.log(`[INFO] Server running on port ${PORT}`);
    });
}

startServer();