require('dotenv').config();
const express = require('express'); // Render stability ke liye zaroori hai
const amqp = require('amqplib');
const mongoose = require('mongoose');
const nodemailer = require('nodemailer');
const NotificationLog = require('./models/NotificationLog');

// Dummy HTTP Server setup taaki Render port crash error na de
const app = express();
const PORT = process.env.PORT || 10000;
app.get('/', (req, res) => res.send('Distributed Worker Engine running seamlessly...'));
app.listen(PORT, () => console.log(`Worker monitoring system active on port ${PORT}`));

const MAIN_QUEUE = 'notifications_v3_queue'; 
const DLX_EXCHANGE = 'notification_dlx_v3';
const RETRY_QUEUE = 'retry_queue_v3';

// 1. Database Connection (Secured using environment variable)
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/nexus_db';
mongoose.connect(MONGO_URI)
  .then(() => console.log('📦 Worker Connected to MongoDB!'))
  .catch((err) => console.error('❌ Worker DB Connection Error:', err));

// 2. Nodemailer Transporter Configuration (Ab credentials environment variables mein hain)
const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: process.env.EMAIL_USER, 
        pass: process.env.EMAIL_PASS       
    }
});

async function startWorker() {
    try {
        // 🔥 Live CloudAMQP Broker Connection URL (Secured via process.env)
        const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
        
        console.log('⏳ Connecting distributed worker to CloudAMQP Broker...');
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();
        
        // Asserting exact topology configurations (DLX Architecture intact)
        await channel.assertExchange(DLX_EXCHANGE, 'direct', { durable: true });
        await channel.assertQueue(MAIN_QUEUE, {
            durable: true,
            arguments: {
                'x-dead-letter-exchange': DLX_EXCHANGE,
                'x-dead-letter-routing-key': RETRY_QUEUE
            }
        });
        
        channel.prefetch(1);
        console.log('🚀 Worker is active and listening directly to CloudAMQP!');

        // 3. Process Messages From The Queue
        channel.consume(MAIN_QUEUE, async (msg) => {
            if (msg !== null) {
                const messageContent = JSON.parse(msg.content.toString());
                console.log(`\n📥 Received message for processing:`, messageContent);

                const { logId, userId, channel: msgChannel, templateType } = messageContent;

                try {
                    if (msgChannel && msgChannel.toUpperCase() === 'EMAIL') {
                        console.log(`📧 Dispatching email to ${userId}...`);

                        // Dynamic Recipient Email routing fallback setup
                        const recipientEmail = messageContent.email || 'nikitachitalkar29@gmail.com';

                        const mailOptions = {
                            from: process.env.EMAIL_USER,     
                            to: recipientEmail,       
                            subject: `NexusNotify - ${templateType}`,
                            text: `Hello ${userId},\n\nYour notification for ${templateType} has been processed successfully via Nexus Distributed System Design!\n\nBest Regards,\nNikita`
                        };

                        const info = await transporter.sendMail(mailOptions);
                        console.log('✨ Email sent successfully! MessageId:', info.messageId);
                    } else {
                        console.log(`ℹ️ Task received for non-email channel [${msgChannel}], logging success internally.`);
                    }

                    // Acknowledge message on absolute success
                    try {
                        await NotificationLog.findByIdAndUpdate(logId, { status: 'SUCCESS' });
                        console.log(`✅ Successfully processed ${templateType}. DB Status Updated.`);
                    } catch (dbErr) {
                        console.log(`⚠️ DB Log update bypassed: ${dbErr.message}`);
                    }
                    channel.ack(msg);

                } catch (error) {
                    console.error(`❌ Processing Error inside Nodemailer: ${error.message}`);

                    const deathHeader = msg.properties.headers && msg.properties.headers['x-death'];
                    const retryCount = deathHeader ? deathHeader[0].count : 0;

                    console.log(`⚠️ Current Retry Count: ${retryCount}`);

                    if (retryCount < 3) {
                        console.log(`🔄 Under retry limit. Dropping to Retry Queue for a 5-second cooldown...`);
                        channel.nack(msg, false, false); 
                    } else {
                        console.log(`🚫 Max retries reached. Marking message as FAILED.`);
                        try {
                            await NotificationLog.findByIdAndUpdate(logId, {
                                status: 'FAILED',
                                errorMessage: `Max retries exhausted. Original error: ${error.message}`
                            });
                        } catch (dbErr) {}
                        channel.ack(msg); 
                    }
                }
            }
        });

    } catch (error) {
        console.error('❌ Worker RabbitMQ Connection Error:', error.message);
    }
}

startWorker();