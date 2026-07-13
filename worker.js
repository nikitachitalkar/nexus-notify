require('dotenv').config();
const amqp = require('amqplib');
const mongoose = require('mongoose');
const nodemailer = require('nodemailer');
const NotificationLog = require('./models/NotificationLog');

// Queue & Exchange Config
const MAIN_QUEUE = 'notifications_v5_queue'; 
const DLX_EXCHANGE = 'notification_dlx_v5';
const DLQ_FINAL = 'dead_letter_queue_v5';

const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/nexus_db';
if (mongoose.connection.readyState === 0) {
    mongoose.connect(MONGO_URI)
      .then(() => console.log('[INFO] Worker connected to MongoDB database.'))
      .catch(() => console.log('[WARN] Worker running in DB-bypass mode.'));
}

// Nodemailer Transporter Setup
const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: process.env.EMAIL_USER, 
        pass: process.env.EMAIL_PASS       
    }
});

async function startWorker() {
    try {
        const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
        
        console.log('[INFO] Connecting worker process to AMQP Broker...');
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();

        // 1. Setup DLX (Dead Letter Exchange)
        await channel.assertExchange(DLX_EXCHANGE, 'direct', { durable: true });

        // 2. Setup Final DLQ
        await channel.assertQueue(DLQ_FINAL, { durable: true });

        // 3. Setup Main Notification Queue linked with DLX
        await channel.assertQueue(MAIN_QUEUE, {
            durable: true,
            arguments: {
                'x-dead-letter-exchange': DLX_EXCHANGE,
                'x-dead-letter-routing-key': DLQ_FINAL
            }
        });

        channel.prefetch(1);
        console.log('[INFO] Worker process active and listening for incoming queue tasks...');

        channel.consume(MAIN_QUEUE, async (msg) => {
            if (msg !== null) {
                const messageContent = JSON.parse(msg.content.toString());
                console.log(`[INFO] Processing payload:`, messageContent);

                const { logId, userId, templateType, email } = messageContent;

                try {
                    // Simulate Failure Test Check
                    if (email === 'fail@test.com') {
                        throw new Error("Simulated Bad Request / Invalid Recipient");
                    }

                    const recipientEmail = email || 'nikitachitalkar29@gmail.com';
                    console.log(`[INFO] Attempting Nodemailer dispatch to recipient: ${recipientEmail}`);

                    const mailOptions = {
                        from: `Nexus Notify <${process.env.EMAIL_USER}>`,     
                        to: recipientEmail,       
                        subject: `NexusNotify Alert - ${templateType}`,
                        text: `Hello ${userId},\n\nYour notification for [${templateType}] was processed successfully.\n\nBest Regards,\nNexus Architecture`
                    };

                    const info = await transporter.sendMail(mailOptions);
                    console.log('[INFO] Email dispatched successfully. Message ID:', info.messageId);

                    if (logId) {
                        try {
                            await NotificationLog.findByIdAndUpdate(logId, { status: 'SUCCESS' });
                        } catch (dbErr) {}
                    }
                    channel.ack(msg); // Acknowledge success

                } catch (error) {
                    console.error(`[ERROR] Processing failed: ${error.message}`);
                    console.log(`[WARN] Rejecting message. Routing payload to Dead Letter Queue (DLQ)...`);
                    
                    if (logId) {
                        try {
                            await NotificationLog.findByIdAndUpdate(logId, { status: 'FAILED_DLQ' });
                        } catch (dbErr) {}
                    }

                    // Reject message without requeue -> CloudAMQP routes it directly to DLQ
                    channel.nack(msg, false, false);
                }
            }
        });

    } catch (error) {
        console.error('[ERROR] Worker connection failure:', error.message);
    }
}

startWorker();